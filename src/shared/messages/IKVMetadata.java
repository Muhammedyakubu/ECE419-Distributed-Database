package shared.messages;

import java.math.BigInteger;

public interface IKVMetadata {

	/**
	 * Adds a server to metadata. Used in metadata reconstruction from string.
	 * @param serverAddPort, startpoint, endpoint
	 */
	public void addServer(String serverAddPort, BigInteger startpoint, BigInteger endpoint);

	/**
	 * Adds server to metadata. Used in ECS. NEED TO IMPLEMENT
	 * @param server, port
	 */
	public void addServer(String server, String port);

	/**
	 * Finds server associated with given key
	 * @param key
	 * @return <ip>:<port> String
	 */
	public String findServer(String key);

	/**
	 * Converts metadata to String
	 * @return metadata String
	 */
	public String toString();

}


