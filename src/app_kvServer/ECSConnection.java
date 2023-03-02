package app_kvServer;

import org.apache.log4j.Logger;
import shared.comms.CommModule;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class ECSConnection implements Runnable{

    private static Logger logger = Logger.getLogger(ECSConnection.class);
    public boolean isOpen;

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
        this.isOpen = true;
    }

    public void run() {
        try {
            output = ecs_socket.getOutputStream();
            input = ecs_socket.getInputStream();

            configureECS();

            // Continously
            while(isOpen) {
                try {
                    KVMessage response = handleECSMessage(CommModule.receiveMessage(ecs_socket));
                    CommModule.sendMessage(response, ecs_socket);

                    /* connection either terminated or lost due to
                     * network problems */
                } catch (IOException ioe) {
                    logger.info("Error! Connection to ECS lost!");
                    isOpen = false;
                    System.exit(1);
                }
            }
        }
        catch(IOException ioe){
            logger.error("Error! Server Connection with ECS could not be established", ioe);
        }
        finally {

            try {
                if (ecs_socket != null) {
                    input.close();
                    output.close();
                    ecs_socket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down ECS-Server connection!", ioe);
            }
        }
    }

    public void configureECS(){
        KVMessage init = new KVMessage(KVMessage.StatusType.CONNECT_ECS, Integer.toString(kvServer.getPort()), kvServer.getHostAddress());
        try {
            CommModule.sendMessage(init,ecs_socket);
        } catch (IOException ioe) {
            logger.info("Error! Connection lost!");
            isOpen = false;
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
                boolean success = kvServer.rebalance(address[1],address[0], values[0] + "," + values[1]);
                if (success)
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



}
