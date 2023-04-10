package app_kvServer;

import org.apache.log4j.Logger;
import shared.comms.CommModule;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending.
 * The class handles the communication completely and handles client requests
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getLogger(ClientConnection.class);
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;
	private String clientID;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 * @param kvServer the server that this client is connected to
	 */
	public ClientConnection(Socket clientSocket, KVServer kvServer) {
		this.clientSocket = clientSocket;
		this.kvServer = kvServer;
		this.isOpen = true;
		this.clientID = null;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			/* NOTE Not sure if I need to send acknowledgement to client, but it's up for discussion */
			
			while(isOpen) {
				try {
					// NOTE: may or may not need to wrap this part in a lock to prevent
					// 			other threads from interrupting protocol messages.
					KVMessage response = handleClientMessage(receiveMessage());
					sendMessage(response);
					
				/* connection either terminated by the client or lost due to 
				 * network problems */
				} catch (IOException ioe) {
					logger.info("Connection to client or server terminated");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
			if (clientID != null) {
				kvServer.clientConnections.remove(clientID);
			}
		}
	}

	/**
	 * Check if the server is stopped
	 * @return true or false
	 */
	private boolean checkStopped() {
		if (kvServer.currStatus == IKVMessage.ServerState.SERVER_STOPPED) return true;
		else return false;
	}

	/**
	 * Handles the client message, performs the corresponding actions and
	 * returns a response to be sent back to the client.
	 *
	 * @param msg the message received from the client
	 * @return the response to be sent to the client
	 */
	public KVMessage handleClientMessage(KVMessage msg) {

		switch (msg.getStatus()) {
			case GET:
				if (checkStopped()){
					return new KVMessage(IKVMessage.StatusType.SERVER_STOPPED, "", "");
				}
				if (!kvServer.isResponsible(msg.getKey()) && !kvServer.isReplicaResponsible(msg.getKey())){
					return new KVMessage(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE, "", "");
				}
				try {
					String value = kvServer.getKV(msg.getKey());
					msg.setValue(value);
					if (value == null)
						msg.setStatus(KVMessage.StatusType.GET_ERROR);
					else
						msg.setStatus(KVMessage.StatusType.GET_SUCCESS);
				} catch (Exception e) {
					logger.error("Error! Key not in key range: " + msg.getKey(), e);
					msg.setStatus(KVMessage.StatusType.GET_ERROR);
				}
				break;
			case PUT:
				if (checkStopped()){
					return new KVMessage(IKVMessage.StatusType.SERVER_STOPPED, "", "");
				}
				if (!kvServer.isResponsible(msg.getKey())){
					return new KVMessage(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE, "", "");
				}
			case SERVER_PUT:
				if (kvServer.currStatus == KVMessage.ServerState.SERVER_WRITE_LOCK){
					msg.setStatus(IKVMessage.StatusType.SERVER_WRITE_LOCK);
					return msg;
				}
				if (!kvServer.isResponsible(msg.getKey()) && !kvServer.isReplicaResponsible(msg.getKey())){
					return new KVMessage(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE, "", "");
				}
				boolean isUpdate = false;
				try {
					// set status for delete
					if (msg.getValue() == null) {
						msg.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
					}

					// do the put
					isUpdate = kvServer.putKV(msg.getKey(), msg.getValue());
					boolean deleteSuccessful = isUpdate && (msg.getValue() == null);

					if (msg.getStatus() == IKVMessage.StatusType.PUT){
						kvServer.replicate(msg.getKey(), msg.getValue());
					}

					// set the status
					if (deleteSuccessful) {
						msg.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
					} else if (isUpdate) {
						msg.setStatus(KVMessage.StatusType.PUT_UPDATE);
					} else {
						msg.setStatus(KVMessage.StatusType.PUT_SUCCESS);
					}
				} catch (Exception e) {
					if (msg.getValue() == null) {
						msg.setStatus(KVMessage.StatusType.DELETE_ERROR);
					} else {
						msg.setStatus(KVMessage.StatusType.PUT_ERROR);
						logger.error("Error! Unable to put value for key: " + msg.getKey(), e);
					}
				}
				break;
			case KEYRANGE:
				msg.setStatus(IKVMessage.StatusType.KEYRANGE_SUCCESS);
				msg.setKey(kvServer.getMetadata().toString());
				msg.setValue("");
				break;

			case KEYRANGE_READ:
				msg.setStatus(IKVMessage.StatusType.KEYRANGE_READ_SUCCESS);
				msg.setKey(kvServer.getMetadata().toKeyRangeReadString());
				msg.setValue("");
				break;
			default:
				logger.error("Error! Invalid message type: " + msg.getStatus());
				msg.setStatus(KVMessage.StatusType.FAILED);
		}
		return msg;
	}
	
	/**
	 * Method sends a KVMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public synchronized void sendMessage(KVMessage msg) throws IOException {
		CommModule.sendMessage(msg, clientSocket);
    }

	/**
	 * Method receives a KVMessage using this socket.
	 * @return the received message as a KVMessage object
	 * @throws IOException some I/O error regarding the input stream
	 */
	private synchronized KVMessage receiveMessage() throws IOException {
		return CommModule.receiveMessage(clientSocket);
    }

	public String getClientID() {
		if (clientID != null) {
			return clientID;
		}

		// clientID has not been set, so get it.
		try {
			KVMessage msg = receiveMessage();
			if (msg.getStatus() == KVMessage.StatusType.CONNECT) {
				clientID = msg.getKey();
				sendMessage(new KVMessage(IKVMessage.StatusType.CONNECT_SUCCESS, "", ""));
			} else if (msg.getStatus() == IKVMessage.StatusType.REQUEST_ID) {
				clientID = this.kvServer.getHostname() + ":"
						 + this.kvServer.getPort() + ":"
						 + this.kvServer.connectionCount.incrementAndGet();
				sendMessage(new KVMessage(IKVMessage.StatusType.SET_CLIENT_ID, clientID, ""));
			}
		} catch (IOException e) {
			logger.error("Error! Unable to receive message from client!", e);
		}
		return clientID;
	}
}
