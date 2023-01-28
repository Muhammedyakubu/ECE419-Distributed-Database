package client;

import shared.messages.KVMessage;

/**
 * I believe this is an interface that the client application will implement...?
 *
 */
public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewMessage(KVMessage msg);

	public void handleStatus(SocketStatus status);
}
