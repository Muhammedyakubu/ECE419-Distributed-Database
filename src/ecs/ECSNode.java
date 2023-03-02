package ecs;

import org.apache.log4j.Logger;
import shared.Range;
import shared.comms.CommModule;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.ServerState;
import shared.messages.KVMessage;
import shared.messages.KVMetadata;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;

public class ECSNode implements IECSNode, Runnable{
    private static final Logger logger = Logger.getLogger(ECSNode.class);
    private InetAddress address;
    private String hostAddress;
    private int port;
    private Range hashRange;
    private Socket socket;
    private boolean running;

    public ECSNode(Socket socket, String hostAddress, int port, Range hashRange) throws IOException {
        this.socket = socket;
        this.hostAddress = hostAddress;
        this.address = InetAddress.getByName(hostAddress);
        this.port = port;
        this.hashRange = hashRange;
        this.running = false;
    }

    public void sendMessage(KVMessage message) {
        try {
            CommModule.sendMessage(message, this.socket);
        } catch (IOException e) {
            logger.debug("Error sending message to " + this.getNodeName());
        }
    }

    public KVMessage receiveMessage() {
        try {
            return CommModule.receiveMessage(this.socket);
        } catch (IOException e) {
            logger.debug("Error receiving message from " + this.getNodeName());
            return null;
        }
    }

    /**
     * @return
     */
    @Override
    public String getNodeName() {
        return this.address.getHostAddress() + ":" + this.port;
    }

    /**
     * @return
     */
    @Override
    public String getNodeHost() {
        return this.hostAddress;
    }

    /**
     * @return
     */
    @Override
    public int getNodePort() {
        return this.port;
    }

    /**
     * @return
     */
    @Override
    public Range getNodeHashRange() {
        return this.hashRange;
    }

    /**
     *
     */
    @Override
    public void run() {

    }

    public void sendMetadata(KVMetadata metadata) {
        sendMessage(new KVMessage(KVMessage.StatusType.UPDATE_METADATA, null, metadata.toString()));
        // TODO: await response?
    }

    /**
     *
     * Need to make sure these protocol messages are sent and received consecutively
     * TODO: Might have to use a lock on the socket
     * @param state
     * @return
     */
    public boolean setState(ServerState state) {
        boolean success = false;
        while (!success) {
            sendMessage(new KVMessage(KVMessage.StatusType.SET_STATE, null, state.toString()));
            KVMessage response = receiveMessage();
            if (response.getStatus() == KVMessage.StatusType.SET_STATE) {
                success = Boolean.parseBoolean(response.getKey());
            } else {
                logger.error("Error setting state of " + this.getNodeName()
                        + " to " + state.toString() + ". Retrying...");
            }
        }
        return success;
    }

}