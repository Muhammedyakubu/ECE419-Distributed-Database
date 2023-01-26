package client;

/**
 * I believe this is an interface that the client application will implement...?
 *
 */
public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewMessae(Message msg);
	
	public void handleStatus(SocketStatus status);
}
