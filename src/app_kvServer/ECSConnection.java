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
        this.close();
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
            case REBALANCE:
                String stripSemiColon = msg.getValue().split(";")[0];
                String[] values = stripSemiColon.split(",");
                String[] address = values[2].split(":");
                int numKeysSent = kvServer.rebalance(address[1],address[0], values[0] + "," + values[1]);
                msg.setKey(Integer.toString(numKeysSent));
                if (numKeysSent >= 0)
                    msg.setStatus(IKVMessage.StatusType.REBALANCE_SUCCESS);
                else
                    msg.setStatus(IKVMessage.StatusType.REBALANCE_ERROR);
                break;
            case SET_STATE:
                kvServer.setState(IKVMessage.ServerState.valueOf(msg.getValue()));
                msg.setKey("true");
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
                ecs_socket.close();
                ecs_socket = null;
            }
        } catch (IOException ioe) {
            logger.error("Error! Unable to tear down ECS-Server connection!", ioe);
        }
    }


}
