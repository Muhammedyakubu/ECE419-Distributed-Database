package app_kvServer;

import org.apache.log4j.Logger;
import shared.comms.CommModule;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

public class ECSConnection implements Runnable{

    private static Logger logger = Logger.getLogger(ECSConnection.class);
    public AtomicBoolean isOpen = new AtomicBoolean(false);

    private Socket ecs_socket;
    private KVServer kvServer;
    private InputStream input;
    private OutputStream output;

    /**
     * Instantiates a connection end point with the ECS
     * @param ecs_socket
     * @param kvServer
     */
    public ECSConnection(Socket ecs_socket, KVServer kvServer){
        this.ecs_socket = ecs_socket;
        this.kvServer = kvServer;
        this.isOpen.set(true);
    }

    public boolean isOpen() {
        return isOpen.get() && ecs_socket != null && !ecs_socket.isClosed();
    }

    public void setIsOpen(boolean isOpen) {
        this.isOpen.set(isOpen);
    }

    public void run() {
        try {
            output = ecs_socket.getOutputStream();
            input = ecs_socket.getInputStream();

            configureECS();

            // Continously
            while(isOpen.get()) {
                try {
                    KVMessage response = handleECSMessage(CommModule.receiveMessage(ecs_socket));
                    CommModule.sendMessage(response, ecs_socket);

                    /* connection either terminated or lost due to
                     * network problems */
                } catch (IOException ioe) {
                    logger.info("Error! Connection to ECS lost!");
                    isOpen.set(false);
                    return;
                }
            }
        }
        catch(IOException ioe){
            logger.error("Error! Server Connection with ECS could not be established", ioe);
        }
        finally {
            this.close();
        }
    }

    public void configureECS(){
        KVMessage init = new KVMessage(KVMessage.StatusType.CONNECT_ECS, Integer.toString(kvServer.getPort()), kvServer.getHostname());
        try {
            CommModule.sendMessage(init,ecs_socket);
        } catch (IOException ioe) {
            logger.info("Error! Connection lost!");
            isOpen.set(false);
        }
    }

    public KVMessage handleECSMessage(KVMessage msg) {
        switch(msg.getStatus()){
            case UPDATE_METADATA:
                kvServer.updateMetadata(msg.getValue());
                msg.setKey("True");
                break;
            case TRANSFER:
                String address = msg.getValue().split(":")[0];
                String port = msg.getValue().split(":")[1];
                int numSent = kvServer.transfer(address, port, msg.getKey());
                msg.setKey(Integer.toString(numSent));
                if (numSent >= 0) {
                    msg.setStatus(IKVMessage.StatusType.TRANSFER_SUCCESS);
                    if (kvServer.cache != null)
                        kvServer.clearCache();
                }
                else
                    msg.setStatus(IKVMessage.StatusType.TRANSFER_ERROR);
                break;
            case DELETE_KEYRANGE:
                int numDeleted = kvServer.deleteKeyrange(msg.getKey());
                msg.setKey(Integer.toString(numDeleted));
                if (numDeleted >= 0) {
                    msg.setStatus(IKVMessage.StatusType.DELETE_KEYRANGE_SUCCESS);
                    if (kvServer.cache != null)
                        kvServer.clearCache();
                }
                else
                    msg.setStatus(IKVMessage.StatusType.DELETE_KEYRANGE_ERROR);
                break;

            case REBALANCE:
                String stripSemiColon = msg.getValue().split(";")[0];
                String[] value = stripSemiColon.split(",");
                String[] addres = value[2].split(":");
                int numKeysSent = kvServer.rebalance(addres[1],addres[0], value[0] + "," + value[1]);
                msg.setKey(Integer.toString(numKeysSent));
                if (numKeysSent >= 0) {
                    msg.setStatus(IKVMessage.StatusType.REBALANCE_SUCCESS);
                    if (kvServer.cache != null)
                        kvServer.clearCache();
                }
                else
                    msg.setStatus(IKVMessage.StatusType.REBALANCE_ERROR);
                break;
            case SET_STATE:
                kvServer.setState(IKVMessage.ServerState.valueOf(msg.getValue()));
                msg.setKey("true");
                break;

            case WAGWAN:
                msg.setKey("Alive!");
                break;

            default:
                logger.error("Error! Invalid ECS Message type: " + msg.getStatus());
                msg.setStatus(KVMessage.StatusType.FAILED);
        }


        return msg;
    }

    public void close() {
        isOpen.set(false);
        try {
            if (ecs_socket != null) {
                logger.info("Closing ECS-Server connection...");
                input.close();
                output.close();
                ecs_socket.close();
                ecs_socket = null;
            }
        } catch (IOException ioe) {
            logger.error("Error! Unable to tear down ECS-Server connection!", ioe);
        }
        // kill the server
        kvServer.close();
    }


}
