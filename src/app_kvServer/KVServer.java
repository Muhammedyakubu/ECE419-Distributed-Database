package app_kvServer;

import app_kvServer.cache.Cache;
import app_kvServer.cache.FIFOCache;
import app_kvServer.cache.LFUCache;
import app_kvServer.cache.LRUCache;
import database.IKVDatabase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.List;


/**
 * This class is the main class of the KVServer application. It provides the
 * functionality to start and stop a server instance. It also provides the
 * functionality to initialize and shut down the storage server.
 *
 * Should this have a main method???
 */
public class KVServer implements IKVServer {
	
	private static Logger logger = Logger.getRootLogger();
	private int port;
	private int cacheSize;
	private CacheStrategy strategy;
	private Cache cache;
	private IKVDatabase db;
	private boolean running;
	private ServerSocket serverSocket;
	// should I use a more efficient data structure?
	private List<ClientConnection> clientConnections;
	
	
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;

		// handle invalid cacheSize and strategy
		if (cacheSize <= 0 || strategy == null) {
			this.strategy = CacheStrategy.None;
		} else {
			this.strategy = CacheStrategy.valueOf(strategy);
		}

		// initialize cache
		switch (this.strategy) {
		case None:
			this.cache = null;
			break;
		case FIFO:
			this.cache = new FIFOCache(cacheSize);
			break;
		case LRU:
			this.cache = new LRUCache(cacheSize);
			break;
		case LFU:
			this.cache = new LFUCache(cacheSize);
			break;
		}

		// TODO: setup db, etc

		run();
	}
	
	@Override
	public int getPort(){
		return port;
	}

	@Override
    public String getHostname(){
		InetAddress ip;
		String hostname;
		try {
			ip = InetAddress.getLocalHost();
			hostname = ip.getHostName();
			System.out.println("Your current IP address : " + ip);
			System.out.println("Your current Hostname : " + hostname);
			return hostname;
		} catch (UnknownHostException e) {
				e.printStackTrace();
		}
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		return this.strategy;
	}

	@Override
    public int getCacheSize(){
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public boolean inCache(String key){
		if (cache == null) {
			return false;
		}
		return cache.contains(key);
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		return "";
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
	}

	@Override
    public void clearCache(){
		cache.clear();
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
	}

	@Override
    public void run() {

		running = initializeServer();

		// handle client connections & stuff
		if (serverSocket != null) {
			while (running) {
				try {
					Socket clientSocket = serverSocket.accept();
					ClientConnection connection =
							new ClientConnection(clientSocket, this);
					clientConnections.add(connection);
					new Thread(connection).start();

				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	/**
	 * The method starts the server thread that waits for incoming client
	 * connections as a background process. The method also starts the cache
	 * replacement strategy if caching is enabled.
	 * @param args
	 *
	 * Someone needs to work on parsing the arguments as specified in the spec:
	 * java -jar m1-server.jar -p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>
	 */
	public static void main(String[] args) {
//		TODO: parse arguments and set defaults
		try {
			new LogSetup("logs/KVserver.log", Level.ALL);
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: java -jar m1-server.jar " +
						"-p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>!");
			} else {
				int port = Integer.parseInt(args[0]);
				new KVServer(port, 10, "FIFO");
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}

}
