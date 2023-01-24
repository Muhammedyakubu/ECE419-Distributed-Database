package app_kvClient;

import client.KVStore;
import client.KVCommInterface;
import logging.LogSetup;
import org.apache.log4j.Logger;

import java.util.*;
import java.lang.*;
import java.io.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "M1Client> ";
    private BufferedReader stdin;
    private KVStore kvstore = null;
    private boolean stop = false;

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
            }
        }
    }

    private void handleCommand(String cmdLine) { //structure taken from m0 and adapted for m1
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                try{
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    connect(serverAddress, serverPort);
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
            } else {
                printError("Invalid number of parameters!");
            }

        } else  if (tokens[0].equals("put")) {
            if(tokens.length == 3) {
                if(/*THERE ARE CLIENTS*/){
                    StringBuilder msg = new StringBuilder();
                    for(int i = 1; i < tokens.length; i++) {
                        msg.append(tokens[i]);
                        if (i != tokens.length -1 ) {
                            msg.append(" ");
                        }
                    }
                    sendMessage(msg.toString());
                } else {
                    printError("Not connected!");
                }
            } else {
                printError("No message passed!");
            }

        } else  if (tokens[0].equals("get")) {

        } else if(tokens[0].equals("disconnect")) {
            disconnect();

        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
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
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("send <text message>");
        sb.append("\t\t sends a text message to the server \n");
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

    private void connect(String address, int port)
            throws UnknownHostException, IOException {
        kvstore = new KVStore(address, port);
        kvstore.addListener(this);
        kvstore.connect();
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return null;
    }
}
