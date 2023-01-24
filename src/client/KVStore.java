package client;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.Message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

/**
 * Looking through echoClient & echoServer, I realize that receiveMessage and
 * sendMessage can be implemented in a separate class. But the similarities
 * between the two are so few that I don't think it's worth it.
 */
public class KVStore implements KVCommInterface {

	private static Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	private String address;
	private int port;


	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(address, port);
		listeners = new HashSet<ClientSocketListener>();
		input = clientSocket.getInputStream();
		output = clientSocket.getOutputStream();
		running = true;
		logger.info("Connection established");
	}

	@Override
	public void disconnect() {
		logger.info("try to close connection ...");

		try {
			running = false;
			logger.info("tearing down the connection ...");
			if (clientSocket != null) {
				clientSocket.close();
				clientSocket = null;
				logger.info("connection closed!");
			}
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(ClientSocketListener.SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		Message msg = new Message(key, value, Message.StatusType.PUT);
		sendMessage(msg); // this should throw an exception if the connection is closed... right?
		Message response = receiveMessage();
		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		Message msg = new Message(key, null, Message.StatusType.GET);
		sendMessage(msg);
		Message response = receiveMessage();
		return response;
	}

	public boolean isRunning() {
		return running;
	}

	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}

	/**
	 * Sends a message to the KVServer.
	 *
	 * @param msg
	 * 		  the message to be sent.
	 * @throws IOException
	 * 		  if the message cannot be sent.
	 */
	public void sendMessage(Message msg) throws IOException {
		byte[] msgBytes = msg.toByteArray();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		// Client specific logging
		logger.info("Send message:\t '" + msg.toString() + "'");

	}

	/**
	 * Receives a message from the KVServer.
	 *
	 * @return the received message.
	 * @throws IOException
	 * 		  if the message cannot be received.
	 */
	public Message receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
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

		/* build final String */
		Message msg = new Message(msgBytes);
		// Client specific logging
		logger.info("Receive message:\t '" + msg.toString() + "'");
		return msg;
	}
}
