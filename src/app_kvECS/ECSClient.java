package app_kvECS;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collection;

import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.Range;
import shared.messages.IKVMessage.ServerState;
import shared.messages.KVMessage;
import shared.messages.KVMetadata;
import shared.comms.CommModule;
import shared.messages.Pair;

public class ECSClient implements IECSClient {
    public static Logger logger = Logger.getLogger(ECSClient.class);
    private int port;
    private InetAddress address;
    public boolean running;
    private ServerSocket ecsSocket;
    private KVMetadata metadata;
    private Map<String, ECSNode> kvNodes;
    private final int SOCKET_TIMEOUT = 100;

    /**
     * Initialize the ECSClient with a given address and port
     * @param address the address of the ecs server
     * @param port the port where the ecs server will listen for server connections
     * @throws UnknownHostException
     */
    public ECSClient(String address, int port) throws UnknownHostException {
        this(address, port, true);
    }
    public ECSClient(String address, int port, boolean run) throws UnknownHostException {
        this.address = (address == null) ? null : InetAddress.getByName(address);
        this.port = port;
        this.running = false;
        this.metadata = new KVMetadata();
        this.kvNodes = new java.util.HashMap<>();
        if (run) run();
    }

    @Override
    public boolean start() {
        return false;
    }

    /**
     * Initialize the ECSClient's server socket
     * @return true if the socket was initialized successfully, false otherwise
     */
    public boolean initialize() {
        logger.info("Initializing ECS ...");
        try {
            if (this.address.equals(null))
                this.ecsSocket = new ServerSocket(port);
            else
                this.ecsSocket = new ServerSocket(port, 10, address);
            ecsSocket.setSoTimeout(SOCKET_TIMEOUT);
            logger.info("ECS listening on port: " + port);
            return true;
        } catch (IOException e) {
            logger.error("Error! Cannot open ECS socket:");
            if(e instanceof java.net.BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            if(e instanceof UnknownHostException){
                logger.error("Bind address could not be found!");
            }
            return false;
        }
    }

    /**
     * Start the ECSClient's server socket and listen for incoming connections
     */
    public void run() {
        running = initialize();

        if (ecsSocket != null) {
            while (running) {
                try {
                    Socket kvSeverSocket = ecsSocket.accept();
                    initializeECSNode(kvSeverSocket);
                } catch (SocketTimeoutException e) {
                    // do nothing
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
                pollNodes();
            }
        }
    }

    public void pollNodes() {
        ArrayList<ECSNode> lostNodes = new ArrayList<>();
        ArrayList<ECSNode> dyingNodes = new ArrayList<>();
        for (ECSNode node: kvNodes.values()) {

            if (node.getAvailableSocketBytes() <= 2) continue;

            KVMessage request = node.receiveMessage();
            if (request == null) {
                lostNodes.add(node);
                continue;
            }

            // should only receive a shutdown message
            if (request.getStatus() == KVMessage.StatusType.SHUTTING_DOWN) {
                dyingNodes.add(node);
                logger.debug("Received shutdown message from " + node.getNodeName()
                                + " adding to dying nodes");
            } else {
                logger.error("Error! Received unexpected message from KVServer");
                node.sendMessage(new KVMessage(
                        KVMessage.StatusType.FAILED,
                        null,
                        "Unexpected message received from KVServer"));
            }
        }
        for (ECSNode node: lostNodes) {
            removeLostNode(node);
        }
        for (ECSNode node: dyingNodes) {
            deleteNode(node);
        }
    }

    /**
     * Forcefully remove a node from the cluster when it is unresponsive
     * @param node
     */
    public void removeLostNode(ECSNode node) {
        logger.info("Removing lost node " + node.getNodeName());
        metadata.removeServer(node.getNodeHost(), node.getNodePort());
        kvNodes.remove(node.getNodeName());
    }

    synchronized public void deleteNode(ECSNode node) {

        kvNodes.remove(node.getNodeName());
        Pair<String, Range> successor = metadata.removeServer(
                node.getNodeHost(),
                node.getNodePort());
        if (successor != null) {
            String successorName = successor.getFirst();
            ECSNode successorNode = kvNodes.get(successorName);
            kvNodes.remove(node.getNodeName());
            rebalance(node, successorNode, true);
        }
        // remove node from kvNodes

        logger.info("Node " + node.getNodeName() + " removed from cluster");
        // if this is the last node, do nothing special?
    }

    public void initializeECSNode(Socket socket) {
        try {
            KVMessage addressMessage = CommModule.receiveMessage(socket);
            if (addressMessage.getStatus() != KVMessage.StatusType.CONNECT_ECS) {
                logger.error("Error! Received unexpected message from KVServer");
                return;
            }
            int serverPort = Integer.parseInt(addressMessage.getKey());
            String serverAddress = addressMessage.getValue();

            // recalculate metadata
            Pair<String, Range> succAndRange = metadata.addServer(serverAddress, serverPort);
            String successor = succAndRange.getFirst();
            Range newNodehashRange = succAndRange.getSecond();
            ECSNode node = new ECSNode(socket, serverAddress, serverPort, newNodehashRange);

            // two checks to see if this is the first node
            // if it is, send the metadata to the node
            if (kvNodes.isEmpty() && successor == null) {
                node.sendMetadata(metadata);
            } else {
                // if it's not the first node, rebalance the metadata
                boolean success = rebalance(kvNodes.get(successor), node, false);
                if (!success) {
                    logger.error("Error! Rebalance from " + successor + " to " + node.getNodeName() + " failed");
                }
            }
            node.setState(ServerState.ACTIVE);
            kvNodes.put(node.getNodeName(), node);
            logger.info("Added node " + node.getNodeName() + " to the cluster");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean rebalance(ECSNode sender, ECSNode receiver, boolean isShutdown) {
        if (sender == null || receiver == null) {
            logger.error("Error! Sender or receiver is null");
            return false;
        }

        receiver.sendMetadata(metadata);
        /**
         * Initiate a rebalance by sending a KVMessage containing with the receiver's name and range
         * in the value field. It has the format "Range,Address:Port;"
         */
        String payload = receiver.getMetadataFormat();
        sender.sendMessage(new KVMessage(KVMessage.StatusType.REBALANCE, null, payload));

        // wait for a rebalance success message.
        KVMessage rebalanceAck = sender.receiveMessage();

        if (rebalanceAck.getStatus() == KVMessage.StatusType.REBALANCE_ERROR) {
            logger.error("Error! Received REBALANCE_ERROR message from KVServer");
            return false;
        }
        else if (rebalanceAck.getStatus() == KVMessage.StatusType.REBALANCE_SUCCESS) {
            logger.debug("Rebalance from " + sender.getNodeName() + " to " + receiver.getNodeName() + " successful");
        }
        else {
            logger.error("Error! Received unexpected message from KVServer");
            return false;
        }

        // update metadata for all (other) servers
        for (ECSNode node : kvNodes.values()) {
            node.sendMetadata(metadata);
        }

        // release write lock if this is not a shutdown (=> server still be active)
        if (!isShutdown) sender.setState(ServerState.ACTIVE);

        return true;
    }

    boolean transferData(ECSNode sender, ECSNode receiver, Range range) {
        if (sender == null || receiver == null || range == null) {
            logger.error("One of the parameters is null");
            return false;
        }
        // update the receiver's metadata
        receiver.sendMetadata(metadata);

        // set a write lock on the sender (may not be necessary if server maintains a write lock)
        sender.setState(ServerState.SERVER_WRITE_LOCK);

        // send a transfer message to the sender
        sender.sendMessage(
                new KVMessage(
                    KVMessage.StatusType.TRANSFER,
                    receiver.getMetadataFormat(),
                    range.toString()
                )
        );

        // wait for a transfer success message.
        KVMessage transferAck = sender.receiveMessage();

        switch (transferAck.getStatus()) {
            case TRANSFER_ERROR:
                logger.error("Transfer error from " + sender.getNodeName() + " to " + receiver.getNodeName());
                return false;
            case TRANSFER_SUCCESS:
                logger.debug("Transfer from " + sender.getNodeName() + " to " + receiver.getNodeName() + " successful");
                break;
            default:
                logger.error("Error! Received unexpected message from KVServer");
                return false;
        }

        // update metadata for all (other) servers
        for (ECSNode node : kvNodes.values()) {
            node.sendMetadata(metadata);
        }

        // release write lock.
        // TODO: should we check for shutdown case? will this function ever be called during shutdown?
        sender.setState(ServerState.ACTIVE);

        return true;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, ECSNode> getNodes() {
        // TODO
        return kvNodes;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return kvNodes.get(Key);
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

    public static String parseCommandLine(String[] args, boolean run_ECS){
        try {
            if (args.length == 0) {
                System.out.println("Error! Missing port number!");
                System.out.println("Usage: java -jar m2-ecs.jar " +
                        "-p <port number> -a <address> -l <logPath> -ll <logLevel> !");
                return "Invalid";
            }
            if(args[0].equals("-h")){
                System.out.println("Usage: java -jar m2-ecs.jar " +
                        "-p <port number> -a <address> -l <logPath> -ll <logLevel> !");
                return "Help printed.";
            }
            //WRONG ARGUMENT ENTRY
            if(args.length % 2 != 0){
                System.out.println("Error! Invalid entry of arguments!");
                System.out.println("Usage: java -jar m2-ecs.jar " +
                        "-p <port number> -a <address> -l <logPath> -ll <logLevel> !");
                return "Invalid";
                //System.exit(0);
            }

            int port_num = -1;
            boolean port_present = false;
            String address = "localhost";
            String logPath = "logs/ecs.log";
            String logLevel = " "; //DEFAULT IS SET TO ALL LATER

            for(int i = 0; i < args.length; i++) {
                //PORT CHECK
                if(args[i].equals("-p")) {
                    port_num = Integer.parseInt(args[i+1]);
                    if(port_num < 0 || port_num > 65535){
                        System.out.println("Error! Port number out of range!");
                        System.out.println("Port number must fall between 0 and 65535, inclusive.");
                        System.exit(0);
                    }
                    port_present = true;
                }

                //ADDRESS CHECK
                if(args[i].equals("-a")) {
                    address = args[i + 1];
                }

                //LOGPATH CHECK
                if(args[i].equals("-l")) {
                    logPath = args[i+1];
                }

                //LOGLEVEL CHECK
                if(args[i].equals("-ll")) {
                    logLevel = args[i+1];
                }
            }

            if(port_present == false) {
                System.out.println("Error! No port number found!");
                System.out.println("Usage: java -jar m2-ecs.jar " +
                        "-p <port number> -a <address> -l <logPath> -ll <logLevel> !");
                return("No port, invalid");
                //System.exit(0);
            }

            Level level = Level.ALL;

            if(!logLevel.equals(" ")){
                level = StringToLevel(logLevel);

                if(level == null){
                    System.out.println("Given loglevel was invalid. Set to default (ALL).");
                    level = Level.ALL;
                }
            }

            //WILL THROW I/O EXCEPTION IF PATH IS INVALID
            if(run_ECS) {
                new LogSetup(logPath, level);

                //LAUNCH ECS HERE WITH SPECIFIED PORT AND ADDRESS!
                new ECSClient(address, port_num).start();
            }

            String returned = "Port: " + port_num + " Address: " + address +
                    " Logpath: " + logPath + " Loglevel: " + logLevel;
            return returned;

        } catch (IOException e) {
            if(e instanceof UnknownHostException){
                System.out.println("Error! Invalid address!");
            } else {
                System.out.println("Error! Unable to find logPath!");
            }
            System.out.println("Usage: java -jar m2-ecs.jar " +
                    "-p <port number> -a <address> -l <logPath> -ll <logLevel> !");
            return "Invalid";
            //e.printStackTrace();
            //System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: java -jar m2-ecs.jar " +
                    "-p <port number> -a <address> -l <logPath> -ll <logLevel> !");
            return "Invalid";
            //System.exit(1);
        }
    }

    public static void main(String[] args) {
        parseCommandLine(args, true);
    }
}
