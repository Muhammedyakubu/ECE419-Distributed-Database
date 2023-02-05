package app_kvClient;

import client.KVStore;
import client.KVCommInterface;
import client.ClientSocketListener;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.util.*;
import java.lang.*;
import java.io.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient, ClientSocketListener {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "M1Client> ";
    private BufferedReader stdin;
    public KVStore kvstore = null;
    private boolean stop = false;
    private String serverAddress;
    private int serverPort;

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

                    try {
                        KVMessage response = (KVMessage) kvstore.put(key, msg.toString());
                        handleNewMessage(response);
                        return response.getStatus().toString();
                    } catch (IOException e){
                        logger.info("Connection to server was lost. Attempting to reconnect...");
                        kvstore.connect();
                        KVMessage response = (KVMessage) kvstore.put(key, msg.toString());
                        handleNewMessage(response);
                        return response.getStatus().toString();
                    }
                } else {
                    printError("Not connected!");
                }
            } else {
                printError("Usage: put <key> <value>!");
            }

        //NEED TO DO
        } else  if (tokens[0].equals("get")) {
            if(tokens.length >= 2) {
                //if there is a connected client
                if(kvstore != null){
                    String key = tokens[1];
                    //RETURNS THE RESPONSE HERE, HANDLE IT
                    try {
                        KVMessage response = (KVMessage) kvstore.get(key);
                        handleNewMessage(response);
                        return response.getStatus().toString();
                    } catch(IOException e){
                        logger.info("Connection to server was lost. Attempting to reconnect...");
                        kvstore.connect();
                        KVMessage response = (KVMessage) kvstore.get(key);
                        handleNewMessage(response);
                        return response.getStatus().toString();
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

        } else if(tokens[0].equals("help")) {
            printHelpText();
        } else {
            printError("Unknown command");
            printHelpText();
        }
        return "";
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

    /**
     * Sets the level of the client logger.
     * @param levelString the desired log level.
     * @return String
     */
    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            String response = "Invalid";
            return response;
        }
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
