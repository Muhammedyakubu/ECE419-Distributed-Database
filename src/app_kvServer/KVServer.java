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
import java.util.LinkedList;
import java.util.List;
import java.lang.*;


/**
 * This class is the main class of the KVServer application. It provides the
 * functionality to start and stop a server instance. It also provides the
 * functionality to initialize and shut down the storage server.
 *
 * Should this have a main method???
 */
public class KVServer implements IKVServer {
	
	public static Logger logger = Logger.getRootLogger();
	private int port;
	private InetAddress bind_address;
	private int cacheSize;
	private CacheStrategy strategy;
	private Cache cache;
	private IKVDatabase db;
	private String dataPath;
	private boolean running;
	private ServerSocket serverSocket;
	// should I use a more efficient data structure?
	private List<ClientConnection> clientConnections = new LinkedList<ClientConnection>();
	
	
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
		this(port, cacheSize, strategy, null, null);
	}
	public KVServer(int port, int cacheSize, String strategy, InetAddress bind_address, String dataPath) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.dataPath = dataPath;
		this.bind_address = bind_address;

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

					logger.info("Connected to " +
							clientSocket.getInetAddress().getHostName() +
							" on port " + clientSocket.getPort());
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

	//ADDRESS GOES IN HERE
	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			if(this.bind_address == null){
				serverSocket = new ServerSocket(this.port);
			} else {
				serverSocket = new ServerSocket(this.port, 50, this.bind_address);
			}
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			if(e instanceof UnknownHostException){
				logger.error("Bind address could not be found!");
			}
			return false;
		}
	}

	private static Level StringToLevel(String levelString) {

		if(levelString.equals(Level.ALL.toString())) {
			return Level.ALL;
		} else if(levelString.equals(Level.DEBUG.toString())) {
			return Level.DEBUG;
		} else if(levelString.equals(Level.INFO.toString())) {
			return Level.INFO;
		} else if(levelString.equals(Level.WARN.toString())) {
			return Level.WARN;
		} else if(levelString.equals(Level.ERROR.toString())) {
			return Level.ERROR;
		} else if(levelString.equals(Level.FATAL.toString())) {
			return Level.FATAL;
		} else if(levelString.equals(Level.OFF.toString())) {
			return Level.OFF;
		} else {
			return null;
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

			//WRONG ARGUMENT ENTRY
			if(args.length % 2 != 0){
				System.out.println("Error! Invalid entry of arguments!");
				System.out.println("Usage: java -jar m1-server.jar " +
						"-p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>!");
				System.exit(0);
			}

			int port = -1;
			boolean port_present = false;
			String address = "localhost";
			String dataPath = " "; //NEED TO SET THIS DEFAULT
			String logPath = "logs/KVserver.log";
			String logLevel = " "; //DEFAULT IS SET TO ALL LATER

			for(int i = 0; i < args.length; i++) {
				//PORT CHECK
				if(args[i].equals("-p")) {
					port = Integer.parseInt(args[i+1]);
					if(port < 0 || port > 65535){
						System.out.println("Error! Port number out of range!");
						System.out.println("Port number must fall between 0 and 65535, inclusive.");
						System.exit(0);
					}
					port_present = true;
				}

				//ADDRESS CHECK
				if(args[i].equals("-a")) {
					address = args[i+1];
				}

				//DATAPATH CHECK
				if(args[i].equals("-d")) {
					dataPath = args[i+1];
				}

				//LOGPATH CHECK
				if(args[i].equals("-l")) {
					logPath = args[i+1];
				}

				//LOGLEVEL CHECK
				if(args[i].equals("-ll")) {
					logLevel = args[i+1];
				}
			}

			if(port_present == false) {
				System.out.println("Error! No port number found!");
				System.out.println("Usage: java -jar m1-server.jar " +
						"-p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>!");
				System.exit(0);
			}

			//WILL THROW UNKNOWN HOST EXCEPTION IF ADDRESS IS INVALID
			InetAddress bind_address = InetAddress.getByName(address);

			Level level = Level.ALL;

			if(!logLevel.equals(" ")){
				 level = StringToLevel(logLevel);

				if(level == null){
					System.out.println("Given loglevel was invalid. Set to default (ALL).");
					level = Level.ALL;
				}
			}

			//WILL THROW I/O EXCEPTION IF PATH IS INVALID
			new LogSetup(logPath, level);

			KVServer server = new KVServer(port, 10, "FIFO", bind_address, dataPath);

				//PASS DATAPATH TO KVSERVER, NEED TO KNOW HOW DB IS IMPLEMENTED
				//PASS ADDRESS, NOT SURE WHERE TO CHANGE THIS...
		} catch (IOException e) {
			if(e instanceof UnknownHostException){
				System.out.println("Error! Invalid logPath <logPath>!");
			} else {
				System.out.println("Error! Unable to  find logPath!");
			}
			System.out.println("Usage: java -jar m1-server.jar " +
					"-p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: java -jar m1-server.jar " +
					"-p <port number> -a <address> -d <dataPath> -l <logPath> -ll <logLevel>!");
			System.exit(1);
		}
	}

}
