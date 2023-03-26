package app_kvClient;

import client.KVStore;
import client.KVCommInterface;
import client.ClientSocketListener;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMetadata;
import shared.messages.Pair;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;
import java.lang.*;
import java.io.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import static shared.MD5.getHash;

public class KVClient implements IKVClient, ClientSocketListener {

    private static Logger logger = Logger.getLogger(KVClient.class);
    private static final String PROMPT = "M1Client> ";
    private BufferedReader stdin;
    public KVStore kvstore = null;
    private boolean stop = false;
    private String serverAddress;
    private int serverPort;
    private KVMetadata metadata = null;

    private boolean deadServer = false;

    /**
     * Runs the client application and takes input from user.
     */
    public void run() { //taken from m0 Application
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            } catch (Exception e){
                logger.warn("The following error occurred:", e);
                printError("Unknown error occurred.");
            }
        }
    }

    public static int getRandomNumberUsingInts(int min, int max) {
        Random random = new Random();
        return random.ints(min, max)
                .findFirst()
                .getAsInt();
    }

    /**
     * Parses user input and takes appropriate actions.
     * @param cmdLine the String command entered by user.
     * @return String
     * @throws Exception
     */
    public String handleCommand(String cmdLine) throws Exception{ //structure taken from m0 and adapted for m1
        String[] tokens = cmdLine.split("\\s+");

        //GOOD
        if(tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        }
        // TODO: remove this once testing is done
        else if (tokens[0].equals("test")) {
            // send n random put requests
            int n = Integer.parseInt(tokens[1]);
            for (int i = 0; i < n; i++) {
                int randomNum = getRandomNumberUsingInts(0, 10000);
                String key = "key" + randomNum;
                String value = "value" + randomNum;
                kvstore.put(key, value);
            }// TODO: remove this once testing is done
        } else if (tokens[0].equals("ps")) {
          // for testing: get the most recent metadata and print the server list only
            handleCommand("keyrange");
            if (metadata != null) {
                for (Pair entry: metadata.metadata) {
                    System.out.println(entry.getFirst());
                }
            }

        } else if (tokens[0].equals("testput")) {
            // send n random put requests
            int n = Integer.parseInt(tokens[1]);
            for (int i = 0; i < n; i++) {
                int randomNum = getRandomNumberUsingInts(0, 10000);
                String key = "key" + randomNum;
                String value = "value" + randomNum;
                String cmd = "put " + key + " " + value;
                handlePut(new String[]{"put", key, value}, cmd);
            }
        // TODO: also remove when done testing
        } else if (tokens[0].equals("cl")) {
            if(tokens.length == 2) {
                try {
                    serverAddress = InetAddress.getLocalHost().getHostAddress();
                    serverPort = Integer.parseInt(tokens[1]);
                    newConnection(serverAddress, serverPort);
                } catch (Exception e) {
                    printError("Invalid command!");
                    logger.warn("Invalid command!", e);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        //GOOD
        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                } catch(NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                }
                catch (Exception e) {
                    printError("Another issue occurred!");
                    logger.warn("Unknown error occurred!", e);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        //GOOD
        } else  if (tokens[0].equals("put")) {
            try {
                return handlePut(tokens, cmdLine);
            } catch (IOException e){
                logger.warn("Connection to server was lost.");
                disconnect();
                metadata.removeServer(serverAddress, serverPort);
                System.out.println("Connected server has crashed. Please try reconnecting one of the listed servers:");
                if (metadata != null && !metadata.isEmpty()) {
                    for (Pair entry: metadata.metadata) {
                        System.out.println(entry.getFirst());
                    }
                }
                deadServer = true;
            } catch (Exception e){
                logger.warn("Exception occurred");
            }



        //NEED TO DO
        } else  if (tokens[0].equals("get")) {
            if(tokens.length >= 2) {
                //if there is a connected client
                if(kvstore != null){
                    String key = tokens[1];

                    String currServerAddPort = serverAddress + ":" + serverPort;
                    String newServerAddPort;
                    String succ1 = currServerAddPort;
                    String succ2 = currServerAddPort;
                    if (metadata != null && !deadServer) {
                        newServerAddPort = metadata.findServer(key);
                        succ1 = metadata.getNthSuccessor(newServerAddPort, 1).getFirst();
                        succ2 = metadata.getNthSuccessor(newServerAddPort, 2).getFirst();
                    }
                    else newServerAddPort = currServerAddPort;
                    deadServer = false;

                    //IF THE CURRENTLY CONNECTED SERVER IS NOT THE CORRECT SERVER OR A REPLICA
                    if(currServerAddPort.compareTo(newServerAddPort) != 0 && currServerAddPort.compareTo(succ1) != 0
                            && currServerAddPort.compareTo(succ2) != 0)
                    {
                        disconnect();
                        String[] IPPort = newServerAddPort.split(":");
                        serverAddress = IPPort[0];
                        serverPort = Integer.parseInt(IPPort[1]);
                        newConnection(serverAddress, serverPort);
                    }

                    try {
                        KVMessage response = (KVMessage) kvstore.get(key);

                        if(response.getStatus() == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE)
                        {
                            handleNotResponsible(cmdLine);
                        }
                        else{ handleNewMessage(response);}
                        return response.getStatus().toString();
                    } catch(IOException e){
                        logger.info("Connection to server was lost.");
                        disconnect();
                        metadata.removeServer(serverAddress, serverPort);
                        System.out.println("Connected server has crashed. Please try reconnecting one of the listed servers:");
                        if (metadata != null && !metadata.isEmpty()) {
                            for (Pair entry: metadata.metadata) {
                                System.out.println(entry.getFirst());
                            }
                        }
                        deadServer = true;

//                        KVMessage response = (KVMessage) kvstore.get(key);
//                        handleNewMessage(response);
//                        if(response.getStatus() == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE)
//                        {
//                            handleNotResponsible(cmdLine);
//                        }
//                        return response.getStatus().toString();
                    }

                } else {
                    printError("Not connected!");
                }
            } else {
                printError("Usage: get <key>!");
            }

        //GOOD
        } else if(tokens[0].equals("disconnect")) {
            disconnect();

        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals("Invalid")) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("keyrange")) {
            //if there is a connected client
            if(kvstore != null){

                try {
                    KVMessage response = (KVMessage) kvstore.getKeyRange(); //NEED TO IMPLEMENT THIS
                    handleNewMessage(response);
                    metadata = null;
                    metadata = new KVMetadata(response.getKey());
                    return response.getStatus().toString();
                } catch(IOException e){
                    logger.info("Connection to server was lost.");
                }

            } else {
                printError("Not connected!");
            }
        } else if(tokens[0].equals("keyrange_read")) {
            //if there is a connected client
            if(kvstore != null){

                try {
                    KVMessage response = (KVMessage) kvstore.getKeyRange(); //NEED TO IMPLEMENT THIS
                    response.setStatus(IKVMessage.StatusType.KEYRANGE_READ_SUCCESS);
                    metadata = null;
                    metadata = new KVMetadata(response.getKey());
                    response.setKey(metadata.toKeyRangeReadString());
                    handleNewMessage(response);
                    return response.getStatus().toString();
                } catch(IOException e){
                    logger.info("Connection to server was lost.");
                }

            } else {
                printError("Not connected!");
            }
        } else if(tokens[0].equals("help")) {
            printHelpText();
        } else {
            printError("Unknown command");
            printHelpText();
        }
        return "";
    }

    public String handlePut(String[] tokens, String cmdLine) throws Exception{
        if(tokens.length >= 2) {
            //if there is a connected client
            if(kvstore != null){
                String key = tokens[1];
                StringBuilder msg = new StringBuilder();
                for(int i = 2; i < tokens.length; i++) {
                    msg.append(tokens[i]);
                    if (i != tokens.length -1 ) {
                        msg.append(" ");
                    }
                }
                String currServerAddPort = serverAddress + ":" + serverPort;
                String newServerAddPort;
                if (metadata != null && !deadServer) {
                    newServerAddPort = metadata.findServer(key);
                }
                else newServerAddPort = currServerAddPort;

                deadServer = false;
                if(currServerAddPort.compareTo(newServerAddPort) != 0)
                {
                    disconnect();
                    String[] IPPort = newServerAddPort.split(":");
                    serverAddress = IPPort[0];
                    serverPort = Integer.parseInt(IPPort[1]);
                    newConnection(serverAddress, serverPort);
                }

                KVMessage response = (KVMessage) kvstore.put(key, msg.toString());
                //handleNewMessage(response);
                if(response.getStatus() == IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE)
                {
                    handleNotResponsible(cmdLine);
                }
                else {handleNewMessage(response);}
                return response.getStatus().toString();

            } else {
                printError("Not connected!");
            }
        } else {
            printError("Usage: put <key> <value>!");
        }
        return "Invalid";
    }

    /**
     * Prints given error.
     * @param error the desired error to be output.
     */
    public void printError(String error){
        System.out.println(PROMPT + "Error: " +  error);
    }

    /**
     * Prints help text
     */
    public void printHelpText(){
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t sends a key-value pair to the server to be stored \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t sends a key to the server to retrieve the corresponding value \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    /**
     * Disconnects the client from the server
     */
    public void disconnect(){
        if (kvstore != null){
            kvstore.disconnect();
            kvstore = null;
        } else {
            printError("No existing connection");
        }
    }

    private static Level StringToLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            return Level.ALL;
        } else if(levelString.equals(Level.DEBUG.toString())) {
            return Level.DEBUG;
        } else if(levelString.equals(Level.INFO.toString())) {
            return Level.INFO;
        } else if(levelString.equals(Level.WARN.toString())) {
            return Level.WARN;
        } else if(levelString.equals(Level.ERROR.toString())) {
            return Level.ERROR;
        } else if(levelString.equals(Level.FATAL.toString())) {
            return Level.FATAL;
        } else if(levelString.equals(Level.OFF.toString())) {
            return Level.OFF;
        } else {
            return null;
        }
    }

    /**
     * Sets the level of the client logger.
     * @param levelString the desired log level.
     * @return String
     */
    private String setLevel(String levelString) {
        Level level = StringToLevel(levelString);
        if(level == null) {
            return "Invalid";
        }

        Logger.getRootLogger().setLevel(level);
        return level.toString();
    }

    /**
     * Prints the possible log levels.
     */
    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    /**
     * Instantiates new functional client and connects it to server
     * @param hostname the server host
     * @param port the server port number
     * @throws Exception
     */
    @Override
    public void newConnection(String hostname, int port) throws Exception{
            logger.info("Connecting to " + hostname + ":" + port);
            kvstore = new KVStore(hostname, port);
            kvstore.connect();
            kvstore.addListener(this);
    }

    /**
     * Returns the KVStore object associated with this client.
     * @return KVCommInterface
     */
    @Override
    public KVCommInterface getStore(){
        return kvstore;
    }

    /**
     * Prints the message returned by the server.
     * @param msg the message returned by the server
     */
    @Override
    public void handleNewMessage(KVMessage msg) {
        if(!stop) {
            System.out.println(msg.toString());
        }
    }

    /**
     * Acts on the socket status.
     * @param status the socket status.
     */
    @Override
    public void handleStatus(SocketStatus status) {
        if(status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.print(PROMPT);
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
        }
    }

    public void handleNotResponsible(String cmdLine){
        String[] tokens = cmdLine.split("\\s+");
        metadata = null;
        try {
            IKVMessage response = kvstore.getKeyRange();
            metadata = new KVMetadata(response.getKey());
            String serverAddPort = metadata.findServer(tokens[1]);
            String[] IPPort = serverAddPort.split(":");
            disconnect();
            serverAddress = IPPort[0];
            serverPort = Integer.parseInt(IPPort[1]);
            newConnection(serverAddress, serverPort);
            logger.info("Connected to new server. Resending request...");
            handleCommand(cmdLine);
        } catch(NumberFormatException nfe) {
            printError("No valid address. Port must be a number!");
            logger.warn("Unable to parse argument <port>", nfe);
        } catch (UnknownHostException e) {
            printError("Unknown Host!");
            logger.warn("Unknown Host!", e);
        } catch (IOException e) {
            printError("Could not establish connection!");
            logger.warn("Could not establish connection!", e);
        } catch (Exception e) {
            printError("Another issue occurred!");
            logger.warn("Unknown error occurred!", e);
        }
    }


    public static void main(String[] args) {
        try {
            new LogSetup("logs/KVClient.log", Level.ALL);
            KVClient client = new KVClient();
            client.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
