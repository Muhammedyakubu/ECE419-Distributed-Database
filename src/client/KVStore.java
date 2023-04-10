package client;

import org.apache.log4j.Logger;
import shared.comms.CommModule;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

/**
 * KVStore provides an API for the client side application to communicate with
 * the KVServer. It implements methods responsible for sending get and put
 * requests to the server, as well as receiving responses from the server.
 * To do this, it also implements communication methods sendMessage and
 * receiveMessage.
 *
 * Looking through echoClient & echoServer, I realize that receiveMessage and
 * sendMessage can be implemented in a separate class. But the similarities
 * between the two are so few that I don't think it's worth it.
 */
public class KVStore implements KVCommInterface {

	private static Logger logger = Logger.getLogger(KVStore.class);
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
		if (address.equals("localhost")){
			try {
				String ip;
				final DatagramSocket socket = new DatagramSocket();
				socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
				this.address = socket.getLocalAddress().getHostAddress();
			}
			catch(Exception e){
				logger.warn("Could not translate localhost to IP", e);
			}
		}
		else
			this.address = address;
		this.port = port;
		listeners = new HashSet<ClientSocketListener>();
	}

	@Override
	public void connect() throws Exception {
		InetAddress address = InetAddress.getByName(this.address);
		clientSocket = new Socket(address, port);
		input = clientSocket.getInputStream();
		output = clientSocket.getOutputStream();
		running = true;
		logger.info("Connection established");
	}

	@Override
	public void disconnect() {
		logger.info("try to close connection ...");

		if (!running || listeners == null) {
			logger.info("connection is already closed.");
			return;
		}

		try {
			running = false;
			logger.info("tearing down the connection ...");
			if (clientSocket != null) {
				input.close();
				output.close();
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
		KVMessage msg = new KVMessage(KVMessage.StatusType.PUT, key, value);
		CommModule.sendMessage(msg, clientSocket); // this should throw an exception if the connection is closed... right?
		KVMessage response = CommModule.receiveMessage(clientSocket);
		return response;
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.GET, key, null);
		CommModule.sendMessage(msg, clientSocket); // this should throw an exception if the connection is closed... right?
		KVMessage response = CommModule.receiveMessage(clientSocket);
		return response;
	}

	public IKVMessage getKeyRange() throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.KEYRANGE, null, null);
		CommModule.sendMessage(msg, clientSocket); // this should throw an exception if the connection is closed... right?
		KVMessage response = CommModule.receiveMessage(clientSocket);
		return response;
	}

	public IKVMessage getKeyRangeRead() throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.KEYRANGE_READ, null, null);
		CommModule.sendMessage(msg, clientSocket); // this should throw an exception if the connection is closed... right?
		KVMessage response = CommModule.receiveMessage(clientSocket);
		return response;
	}

	public IKVMessage getClientID() throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.REQUEST_ID, null, null);
		CommModule.sendMessage(msg, clientSocket);
		KVMessage response = CommModule.receiveMessage(clientSocket);
		return response;
	}

	public IKVMessage sendClientID(String clientID) throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.CONNECT, clientID, null);
		CommModule.sendMessage(msg, clientSocket);
		KVMessage response = CommModule.receiveMessage(clientSocket);
		return response;
	}

	public IKVMessage subscribe(String key) throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.SUBSCRIBE, key, null);
		CommModule.sendMessage(msg, clientSocket);
		KVMessage response = CommModule.receiveMessage(clientSocket);
		return response;
	}

	public IKVMessage unsubscribe(String key) throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.UNSUBSCRIBE, key, null);
		CommModule.sendMessage(msg, clientSocket);
		KVMessage response = CommModule.receiveMessage(clientSocket);
		return response;
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
	public void sendMessage(KVMessage msg) throws IOException {
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
	public KVMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while(read != 13 && read != -1 && reading) {/* CR, LF, error */
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
		KVMessage msg = new KVMessage(msgBytes);
		// Client specific logging
		logger.info("Receive message:\t '" + msg.toString() + "'");
		return msg;
	}
}
