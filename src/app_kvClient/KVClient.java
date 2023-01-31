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
    private KVStore kvstore = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

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

    private void handleCommand(String cmdLine) throws Exception{ //structure taken from m0 and adapted for m1
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
                    //RETURNS THE RESPONSE HERE, HANDLE IT
                    KVMessage response = (KVMessage) kvstore.put(key, msg.toString());
                    handleNewMessage(response);
                } else {
                    printError("Not connected!");
                }
            } else {
                printError("Must enter <key>!");
            }

        //NEED TO DO
        } else  if (tokens[0].equals("get")) {
            if(tokens.length >= 2) {
                //if there is a connected client
                if(kvstore != null){
                    String key = tokens[1];
                    //RETURNS THE RESPONSE HERE, HANDLE IT
                    KVMessage response = (KVMessage) kvstore.get(key);
                    handleNewMessage(response);

                } else {
                    printError("Not connected!");
                }
            } else {
                printError("Must enter <key>!");
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
    }

    public void printError(String error){
        System.out.println(PROMPT + "Error: " +  error);
    }

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

    public void disconnect(){
        if (kvstore != null){
            kvstore.disconnect();
            kvstore = null;
        } else {
            printError("No existing connection");
        }
    }

    /*
    private void sendMessage(String msg){
        try {
            kvstore.sendMessage(new Message(msg));
        } catch (IOException e) {
            printError("Unable to send message!");
            disconnect();
        }
    }
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

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception{
            kvstore = new KVStore(hostname, port);
            kvstore.connect();
            kvstore.addListener(this);

    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return kvstore;
    }

    @Override
    public void handleNewMessage(KVMessage msg) {
        if(!stop) {
            System.out.println(msg.toString());
            System.out.print(PROMPT);
        }
    }

    @Override
    public void handleStatus(SocketStatus status) {
        if(status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient client = new KVClient();
            client.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
