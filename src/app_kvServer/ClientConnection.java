package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 *
 * The class handles the communication completely and handles client requests
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 * @param kvServer the server that this client is connected to
	 */
	public ClientConnection(Socket clientSocket, KVServer kvServer) {
		this.clientSocket = clientSocket;
		this.kvServer = kvServer;
		this.isOpen = true;
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
					KVMessage response = handleClientMessage(receiveMessage());
					sendMessage(response);
					
				/* connection either terminated by the client or lost due to 
				 * network problems */
				} catch (IOException ioe) {
					logger.info("Error! Connection lost!");
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
		}
	}

	public KVMessage handleClientMessage(KVMessage msg) {
		switch (msg.getStatus()) {
			case GET:
				try {
					String value = kvServer.getKV(msg.getKey());
					msg.setValue(value);
					msg.setStatus(KVMessage.StatusType.GET_SUCCESS);
				} catch (Exception e) {
					msg.setStatus(KVMessage.StatusType.GET_ERROR);
					logger.error("Error! Unable to get value for key: " + msg.getKey(), e);
				}
				break;
			case PUT:
				try {
					if (msg.getValue() == null || msg.getValue().equals("") || msg.getValue().equals("null")) {
						msg.setValue(null);
						msg.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
					} else {
						msg.setStatus(KVMessage.StatusType.PUT_SUCCESS);
					}
					kvServer.putKV(msg.getKey(), msg.getValue());
				} catch (Exception e) {
					if (msg.getValue() == null) {
						msg.setStatus(KVMessage.StatusType.DELETE_ERROR);
						logger.error("Error! Unable to delete key: " + msg.getKey(), e);
					} else {
						msg.setStatus(KVMessage.StatusType.PUT_ERROR);
						logger.error("Error! Unable to put value for key: " + msg.getKey(), e);
					}
				}
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
	public void sendMessage(KVMessage msg) throws IOException {
		byte[] msgBytes = msg.toByteArray();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg +"'");
    }

	private KVMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;

		while(read != 13 && read != -1 && reading) {/* CR, disconnect, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;

		/* Check for empty message indicating a disconnect */
		if(msgBytes.length == 0) {
			throw new IOException("Error! Connection lost!");
		}
		
		/* build final String */
		KVMessage msg = new KVMessage(msgBytes);
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg + "'");
		return msg;
    }

}
