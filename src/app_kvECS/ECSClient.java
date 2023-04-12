package app_kvECS;

import java.io.IOException;
import java.math.BigInteger;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import ecs.ECSNode;
import ecs.IECSNode;
import ecs.Notification;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.Range;
import shared.messages.IKVMessage;
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
    private final int BACKLOG = 50;
    BigInteger HASH_MAX = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
    private final Range FULL_RANGE = new Range(BigInteger.ZERO, HASH_MAX);
    private final Queue<Notification> notificationQueue = new ConcurrentLinkedQueue<>();

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
            if (this.address.equals(null) || this.address.isLoopbackAddress()) {
                try {
                    String ip;
                    final DatagramSocket socket = new DatagramSocket();
                    socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
                    ip = socket.getLocalAddress().getHostAddress();
                    this.address = InetAddress.getByName(ip);
                } catch (Exception e) {
                    logger.warn("Error in hostname to IP translation", e);
                }
                this.ecsSocket = new ServerSocket(port, BACKLOG, address);
            }
            else
                this.ecsSocket = new ServerSocket(port, BACKLOG, address);
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
                    initializeECSNodeWithReplica(kvSeverSocket);
                } catch (SocketTimeoutException e) {
                    // do nothing
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
                pollNodes();
                dispatchNotifications();
            }
        }
    }

    public ArrayList<String> getClientList(String clientListString) {
        if (clientListString == null) {
            return new ArrayList<>();
        } else {
            return  new ArrayList<>(Arrays.asList(clientListString.split(",")));
        }
    }

    public String getClientListString(List<String> clientList) {
        return clientList.toString().replace("[", "").replace("]", "");
    }

    /**
     * Forwards available notifications to all nodes
     */
    public void dispatchNotifications() {
        Iterator<Notification> it = notificationQueue.iterator();
        while (it.hasNext()) {
            Notification notification = it.next();
            List<String> clientList = getClientList(notification.getMessage().getValue());
            for (ECSNode node: kvNodes.values()) {
                try {
                    node.sendMessage(notification.getMessage());
                    KVMessage response = node.receiveMessage();
                    if (response.getStatus() == IKVMessage.StatusType.NOTIFY_SUBSCRIBERS_SUCCESS) {
                        List<String> notifiedClients = getClientList(response.getValue());
                        clientList.removeAll(notifiedClients);   // may throw null pointer exception
                        // could update list of clients to be notified here.
                        if (clientList.isEmpty()) {
                            break;
                        }
                    } else {
                        logger.error("Received unexpected response from " + node.getNodeName() + ": " + response.getStatus());
                    }
                } catch (IOException e) {
                    logger.error("Error! Unable to send notification to " + node.getNodeName());
                }
            }

            // send response message to notification initiator
            KVMessage notifResponse;
            // if there are any unnotified clients, update then we assume they've disconnected
            if (!clientList.isEmpty()) {

                notifResponse = new KVMessage(
                        KVMessage.StatusType.UNSUBSCRIBE_CLIENTS,
                        notification.getMessage().getKey(),
                        getClientListString(clientList)
                );
            } else {
                notifResponse = new KVMessage(KVMessage.StatusType.NOTIFY_SUBSCRIBERS_SUCCESS, "", "");
            }
            try {
                notification.getInitiator().sendMessage(notifResponse);
                notification.getInitiator().receiveMessage();   // should expect a response, but we don't care what it is
            } catch (IOException e) {
                logger.error("Error! Unable to send notification response to " + notification.getInitiator().getNodeName());
            }

            it.remove();
        }
    }

    public void pollNodes() {
        for (ECSNode node: kvNodes.values()) {
            KVMessage heartbeat = new KVMessage(KVMessage.StatusType.WAGWAN,"", "");
            try {
                node.sendMessage(heartbeat);
                node.receiveMessage();
            } catch (IOException e) {
                logger.error("Error! Unable to send heartbeat to " + node.getNodeName());
            }
        }
        // use iterator to remove nodes
        Iterator<Map.Entry<String, ECSNode>> it = kvNodes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> entry = it.next();
            ECSNode node = entry.getValue();
            if (node.failed()) {
                logger.debug("Node " + node.getNodeName() + " failed, removing from cluster");
                try {
                    removeServer(node, true);
                } catch (Exception e) {
                    logger.error("Error removing failed node: " + node.getNodeName());
                }
                // remove node from kvNodes
                it.remove();
            }
        }
    }

    public void initializeECSNodeWithReplica(Socket socket) {
        KVMessage addressMessage = null;
        try {
            addressMessage = CommModule.receiveMessage(socket);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (addressMessage.getStatus() != KVMessage.StatusType.CONNECT_ECS) {
            logger.error("Error! Received unexpected message from KVServer");
            return;
        }
        int serverPort = Integer.parseInt(addressMessage.getKey());
        String serverAddress = addressMessage.getValue();

        ECSNode node = null;
        try {
            node = new ECSNode(socket, serverAddress, serverPort, notificationQueue); // hash range will be set later
            addServer(node);
        } catch (IOException e) {
            String nodeName = node == null ? "null" : node.getNodeName();
            logger.error("Error initializing ECSNode " + nodeName);
            e.printStackTrace();
        }
    }

    /** We assume this runs with no server failures
     * let it throw exceptions to terminate the operation in the case of failure
     */
    public boolean addServer(ECSNode node) throws IOException {
        // if there are < 3 nodes, have all the nodes replicate to the new node
        metadata.addServer(node.getNodeHost(), node.getNodePort());
        kvNodes.put(node.getNodeName(), node);
        node.sendMetadata(metadata);    // this updates the ECSNode's hash range as well

        if (metadata.size() == 1) { // first node, we're done

        } else if (metadata.size() == 2 || metadata.size() == 3) { // second node, we need to replicate from the first node
            // successor will have all data in the storage service, so we only need to transfer from it
            ECSNode firstNode = kvNodes.get(metadata.getNthSuccessor(node.getNodeName(), -1).getFirst());
            transferData(firstNode, node, FULL_RANGE);
        } else if (metadata.size() > 3) {
            // transfer node's core and all replicated data from successor to new node
            // that includes all data from the 2nd predecessor to the current node
            ECSNode successor = kvNodes.get(metadata.getNthSuccessor(node.getNodeName(), 1).getFirst());
            Pair<String, Range> secondPredecessor = metadata.getNthSuccessor(node.getNodeName(), -2);
            Range allData = new Range(secondPredecessor.getSecond().start, node.getNodeHashRange().end);
            transferData(successor, node, allData);

            // successor deletes its 3rd predecessor's data
            successor.deleteKeyrange(secondPredecessor.getSecond());

            // 2nd successor deletes its 3rd predecessor's data
            ECSNode secondSuccessor = kvNodes.get(metadata.getNthSuccessor(node.getNodeName(), 2).getFirst());
            Pair<String, Range> predecessor = metadata.getNthSuccessor(node.getNodeName(), -1);
            secondSuccessor.deleteKeyrange(predecessor.getSecond());

            // 3rd successor deletes its 3rd predecessor's data (this is the node that was just added)
            ECSNode thirdSuccessor = kvNodes.get(metadata.getNthSuccessor(node.getNodeName(), 3).getFirst());
            thirdSuccessor.deleteKeyrange(node.getNodeHashRange());
        }

        // update metadata for all other nodes
        for (ECSNode kvNode: kvNodes.values()) {
            if (kvNode.getNodeName().equals(node.getNodeName())) continue;
            kvNode.sendMetadata(metadata);
        }

        node.setState(ServerState.ACTIVE);

        return true;
    }

    /** We assume this runs with no server failures
     * let it throw exceptions to terminate the operation in the case of failure
     */
    public boolean removeServer(ECSNode node, boolean isFailure) throws IOException {
        // make a copy of the metadata to get the relative positions of other nodes
        KVMetadata oldMetadata = new KVMetadata(metadata.toString());
        metadata.removeServer(node.getNodeHost(), node.getNodePort());  // update metadata


        if (!isFailure) node.setState(ServerState.SERVER_WRITE_LOCK);
        // if there are <= 3 nodes, we don't need to do anything
        if (metadata.size() >= 3) {
            // transfer core data from node to its 3rd successor
            // offload this responsibility to the node's successor so this function can be
            // reused in the case of a failure
            ECSNode successor = kvNodes.get(oldMetadata.getNthSuccessor(node.getNodeName(), 1).getFirst());
            ECSNode thirdSuccessor = kvNodes.get(oldMetadata.getNthSuccessor(node.getNodeName(), 3).getFirst());
            transferData(successor, thirdSuccessor, node.getNodeHashRange());

            // transfer predecessor's data to node's 2nd successor
            ECSNode secondSuccessor = kvNodes.get(oldMetadata.getNthSuccessor(node.getNodeName(), 2).getFirst());
            ECSNode predecessor = kvNodes.get(oldMetadata.getNthSuccessor(node.getNodeName(), -1).getFirst());
            transferData(predecessor, secondSuccessor, predecessor.getNodeHashRange());

            // transfer 2nd predecessor's data to node's successor
            ECSNode secondPredecessor = kvNodes.get(oldMetadata.getNthSuccessor(node.getNodeName(), -2).getFirst());
            transferData(secondPredecessor, successor, secondPredecessor.getNodeHashRange());
        }
        if (!isFailure) node.deleteKeyrange(node.getNodeHashRange());   // delete all data in the node's range

        for (ECSNode kvNode: kvNodes.values()) {
            if (kvNode.getNodeName().equals(node.getNodeName())) continue;    // won't be in kvNodes anymore
            kvNode.sendMetadata(metadata);
        }

        if (!isFailure) node.setState(ServerState.SERVER_STOPPED);

        return true;
    }

    /**
     * Transfer data from one node to another
     * @param sender - the node to transfer data from
     * @param receiver - the node to transfer data to
     * @param range - the hash-range of data to transfer
     * @return true if the transfer was successful, false otherwise
     */

    boolean transferData(ECSNode sender, ECSNode receiver, Range range) throws IOException {
        if (sender == null || receiver == null || range == null) {
            logger.error("One of the parameters is null");
            return false;
        }

        receiver.sendMetadata(metadata);    // update receiver's metadata so it knows what to expect

        // send a transfer message to the sender
        sender.sendMessage(
                new KVMessage(
                    KVMessage.StatusType.TRANSFER,
                    range.toString(),
                    receiver.getNodeName()
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
            String address = InetAddress.getLocalHost().getHostAddress();
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
