package client;

import shared.messages.Message;

/**
 * I believe this is an interface that the client application will implement...?
 *
 */
public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};

	//DISCUSS
	public void handleNewMessage(Message msg);
	
	public void handleStatus(SocketStatus status);
}
