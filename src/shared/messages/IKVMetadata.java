package shared.messages;

import java.math.BigInteger;

public interface IKVMetadata {

	public void addServer(String serverAddPort, BigInteger startpoint, BigInteger endpoint);

	public void addServer(String server, String port);

	public String findServer(String key);

	public String toString();

}


