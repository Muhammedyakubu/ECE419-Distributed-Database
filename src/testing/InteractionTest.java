package testing;

import app_kvServer.KVServer;
import logger.LogSetup;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.junit.Assert.*;
import org.junit.*;

import client.KVStore;
import app_kvClient.KVClient;
import junit.framework.TestCase;
import shared.messages.IKVMessage;
import shared.messages.IKVMessage.StatusType;

import java.io.IOException;


public class InteractionTest extends TestCase{
	// This doesn't work right now. We need to create a private KVServer separate from the test suite's
	private KVStore kvClient;
	private static KVServer kvServer;
	private static Thread serverThread;
	private KVClient client_app;
	private static boolean setup = false;

	public void setUpServer() {
		if (setup) return;
		try {
			new LogSetup("logs/testing/test.log", Level.ALL);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setup = true;

		System.out.println("Starting server...");
		kvServer = new KVServer(50000, 10, "None", false);
		serverThread = new Thread(new Runnable() {
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
	}

	@Before
	public void setUp() {
		setUpServer();
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@After
	public void tearDown() {
		kvClient.disconnect();
		kvServer.clearStorage();
	}


	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		Assert.assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		Assert.assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}

		Assert.assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		IKVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		Assert.assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		IKVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		Assert.assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an_unset_value";
		IKVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		Assert.assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

	@Test
	public void testPutAfterServerReconnection() {
		String command = "put this here";
		String command2 = "put something here";
		String response = null;
		Exception ex = null;
		client_app = new KVClient();

		try {
			client_app.kvstore = this.kvClient;
			//client_app.handleCommand("connect localhost 5000");
			client_app.handleCommand(command);
			kvServer.kill();
			setUpServer();
			response = client_app.handleCommand(command2);
		} catch (Exception e) {
			ex = e;
		}
		Assert.assertTrue(ex == null && response.equals(StatusType.PUT_SUCCESS.toString()));
	}
	@Test
	public void testGetAfterServerReconnection() {
		String command = "put this here";
		String command2 = "get this";
		String response = null;
		Exception ex = null;
		client_app = new KVClient();

		try {
			client_app.kvstore = this.kvClient;
			//client_app.handleCommand("connect localhost 5000");
			client_app.handleCommand(command);
			kvServer.kill();
			setUpServer();
			response = client_app.handleCommand(command2);
		} catch (Exception e) {
			ex = e;
		}
		Assert.assertTrue(ex == null && response.equals(StatusType.GET_SUCCESS.toString()));
	}

	/* Our tests start here */

//	@Test
//	public void testKVWithSpaces() {
//		String key = "foo bar";
//		String value = "bar foo";
//		IKVMessage response = null;
//		Exception ex = null;
//
//		try {
//			response = kvClient.put(key, value);
//		} catch (Exception e) {
//			ex = e;
//		}
//
//		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
//	}

}
