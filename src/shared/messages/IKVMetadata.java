package shared.messages;

import shared.Range;

import java.math.BigInteger;

public interface IKVMetadata {

	/**
	 * Adds a server to metadata. Used in metadata reconstruction from string.
	 * @param serverAddPort, startpoint, endpoint
	 */
	public void addServer(String serverAddPort, BigInteger startpoint, BigInteger endpoint);

	/**
	 * adds a new server to the metadata and returns the new server's
	 * keyrange and its successor's address info
	 * This function will also have to deal with when the new server is the
	 * first server, in which case it will return null for the successor's
	 * address info
	 *
	 * @param server the address of the server
	 * @param port          the port the server is listening on
	 * @return a pair containing the new server's keyrange and its successor
	 */
	public Pair<String, Range> addServer(String server, int port);

	/**
	 * removes a server from the metadata and returns the successor's
	 * keyrange and address info
	 *
	 * @param serverAddress the address of the server
	 * @param port          the port the server is listening on
	 * @return a pair containing the removed server's successor's <addr>:<port> and the successor's keyrange
	 */

	Pair<String, Range> removeServer(String serverAddress, int port);

	/**
	 * Finds server associated with given key
	 * @param key
	 * @return <ip>:<port> String
	 */
	public String findServer(String key);

	/**
	 * Finds range associated with given server
	 * @param serverAddPort
	 * @return Range
	 */
	public Range getRange(String serverAddPort);


	/**
	 * Converts metadata to String
	 * @return metadata String
	 */
	public String toString();

}


