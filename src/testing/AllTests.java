package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						new KVServer(50000, 10, "FIFO");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		//clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(KVMessageTest.class);
		clientSuite.addTestSuite(KVServerTest.class);
		clientSuite.addTestSuite(KVDatabaseTest.class);
		clientSuite.addTestSuite(FIFOCacheUnitTest.class);
		//clientSuite.addTestSuite(FIFOCacheTest.class);
		clientSuite.addTestSuite(LRUCacheUnitTest.class);
		//clientSuite.addTestSuite(LRUCacheTest.class);
		clientSuite.addTestSuite(KVMetadataTest.class);
		return clientSuite;
	}
	
}
