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

public class ECSNode implements IECSNode{
    private static final Logger logger = Logger.getLogger(ECSNode.class);
    private InetAddress address;
    private String hostAddress;
    private int port;
    private Range hashRange;
    private Socket socket;
    private boolean failed = false;

    public ECSNode(Socket socket, String hostAddress, int port, Range hashRange) throws IOException {
        this.socket = socket;
        this.hostAddress = hostAddress;
        this.address = InetAddress.getByName(hostAddress);
        this.port = port;
        this.hashRange = hashRange;
    }

    public void sendMessage(KVMessage message) {
        try {
            CommModule.sendMessage(message, this.socket);
        } catch (IOException e) {
            logger.debug("Error sending message to " + this.getNodeName());
            failed = true;
        }
    }

    public KVMessage receiveMessage() {
        try {
            return CommModule.receiveMessage(this.socket);
        } catch (IOException e) {
            logger.debug("Error receiving message from " + this.getNodeName());
            failed = true;
            return null;
        }
    }

    public boolean failed() {
        return failed;
    }

    /**
     * @return
     */
    @Override
    public String getNodeName() {
        return this.hostAddress + ":" + this.port;
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

    public void updateNodeHashRange(KVMetadata metadata) {
        Range hr = metadata.getRange(this.getNodeName());
        if (this.hashRange == null || (hr != null && !hr.equals(this.hashRange))) {
            this.hashRange = hr;
        }
    }

    public boolean deleteKeyrange(Range range) {
        sendMessage(new KVMessage(KVMessage.StatusType.DELETE_KEYRANGE, range.toString(), null));
        KVMessage response = receiveMessage();
        if (response.getStatus() != KVMessage.StatusType.DELETE_KEYRANGE_SUCCESS) {
            logger.error("Data was not deleted from " + this.getNodeName());
            return false;
        }
        return true;
    }

    public int getAvailableSocketBytes() {
        try {
            return socket.getInputStream().available();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getMetadataFormat() {
        return this.hashRange.toString() + "," + this.getNodeName() + ";";
    }

    public void sendMetadata(KVMetadata metadata) {
        sendMessage(new KVMessage(KVMessage.StatusType.UPDATE_METADATA, null, metadata.toString()));

        // update local metadata
        this.updateNodeHashRange(metadata);

        // await response
        KVMessage response = receiveMessage();
        boolean success = Boolean.parseBoolean(response.getKey());

        if (!success ||
                response.getStatus() != KVMessage.StatusType.UPDATE_METADATA) {
            logger.error("Metadata was not acknowledged by " + this.getNodeName());
            logger.error(response.getStatus() + response.getKey() + response.getValue());
            // TODO: throw exception? or change return type to boolean?
        }
    }

    /**
     *
     * Need to make sure these protocol messages are sent and received consecutively
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
