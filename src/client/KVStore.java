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
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

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
	private final Queue<KVMessage> messageQueue = new ConcurrentLinkedQueue<>();

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
		sendMessage(msg); // this should throw an exception if the connection is closed... right?
		KVMessage response = receiveMessage();
		return response;
	}

	@Override
	public IKVMessage get(String key) throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.GET, key, null);
		sendMessage(msg); // this should throw an exception if the connection is closed... right?
		KVMessage response = receiveMessage();
		return response;
	}

	public IKVMessage getKeyRange() throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.KEYRANGE, null, null);
		sendMessage(msg); // this should throw an exception if the connection is closed... right?
		KVMessage response = receiveMessage();
		return response;
	}

	public IKVMessage getKeyRangeRead() throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.KEYRANGE_READ, null, null);
		sendMessage(msg); // this should throw an exception if the connection is closed... right?
		KVMessage response = receiveMessage();
		return response;
	}

	public IKVMessage getClientID() throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.REQUEST_ID, null, null);
		sendMessage(msg);
		KVMessage response = receiveMessage();
		return response;
	}

	public IKVMessage sendClientID(String clientID) throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.CONNECT, clientID, null);
		sendMessage(msg);
		KVMessage response = receiveMessage();
		return response;
	}

	public IKVMessage subscribe(String key) throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.SUBSCRIBE, key, null);
		sendMessage(msg);
		KVMessage response = receiveMessage();
		return response;
	}

	public IKVMessage unsubscribe(String key) throws Exception {
		KVMessage msg = new KVMessage(KVMessage.StatusType.UNSUBSCRIBE, key, null);
		sendMessage(msg);
		KVMessage response = receiveMessage();
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
		CommModule.sendMessage(msg, clientSocket);
	}


	public int getInputAvailable(){
		int avail = -1;
		try{
			if(input != null)
				avail = input.available();
		} catch (IOException e){
			System.out.println(e);
		}
		return avail;
	}

	public KVMessage receiveMessage() throws IOException {
		return receiveMessage(false);
	}

	/**
	 * Receives a message from the KVServer.
	 *
	 * @return the received message.
	 * @throws IOException
	 * 		  if the message cannot be received.
	 */
	public synchronized KVMessage receiveMessage(boolean notifThread) throws IOException {
		if (notifThread) {
			String deleted_notif = handleNewNotifications();
			if(deleted_notif != null) {
				KVMessage msg = new KVMessage(IKVMessage.StatusType.NOTIFY, deleted_notif, "");
				return msg;
			}
			return null;
		}

		KVMessage msg = messageQueue.poll();
		if (msg != null) {
			return msg;
		}
	    msg = CommModule.receiveMessage(clientSocket);
		if (msg.getStatus() == IKVMessage.StatusType.NOTIFY) {
			handleNotification(msg);
			return receiveMessage();
		}
		return msg;
	}

	//FOR DELETE, RETURN SOMETHING FROM THIS AND HANDLE IN receiveMessage
	public String handleNewNotifications() throws IOException {
		KVMessage msg = CommModule.receiveMessage(clientSocket);
		if (msg.getStatus() == IKVMessage.StatusType.NOTIFY) {
			handleNotification(msg);
			if(msg.getKey().substring(0,6).equals("DELETE"))
				return msg.getKey().substring(8);
			return null;
		} else {
			messageQueue.add(msg);
			return null;
		}
	}

	public void handleNotification (KVMessage msg) {
		for(ClientSocketListener listener : listeners) {
//			listener.handleNotification(msg);
			String key = msg.getKey().substring(8);
			String operation = msg.getKey().substring(0,6);
			logger.debug("Received notify message for key: " + key);
			//System.out.println(operation);
			if(operation.equals("DELETE"))
				System.out.println("NOTIFICATION: Key " + key + " was deleted.");
			else
				System.out.println("NOTIFICATION: Key " + key + " was updated.");
			System.out.print("M4Client> ");
		}
	}
}
