package shared;

import app_kvClient.IKVClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;

public final class ObjectFactory {
	/*
	 * Creates a KVClient object for auto-testing purposes
	 */
    public static IKVClient createKVClientObject() {
        // TODO Auto-generated method stub
    	return null;
    }
    
    /*
     * Creates a KVServer object for auto-testing purposes
     */
	public static IKVServer createKVServerObject(int port, int cacheSize, String strategy) {
		final IKVServer kvServer = new KVServer(port, cacheSize, strategy, false);
		Thread serverThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					kvServer.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		serverThread.start();

		// give the server some time to start up
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return kvServer;
	}
}