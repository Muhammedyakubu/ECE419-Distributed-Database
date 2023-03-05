package testing.noJUnit;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import static shared.messages.IKVMessage.StatusType.GET_SUCCESS;

/**
 * This will ensure that everything to be tested in the Milestone 2 Demo works.
 */
public class M2DemoTests extends TestCase {
    private static boolean setup = false;
    private static Thread ecsThread;
    private static Map<String, KVServer> servers = new HashMap<>();
    private static ECSClient ecs;
    private KVStore client;
    private KVClient client_app;
    private final static int ECS_PORT = 11111;
    private final static String ECS_ADDRESS = "localhost";
    private final static String DATA_PATH = "./src/KVStorage";
    private final static int NUM_KEYS = 10;
    private final static int SERVER_START_PORT = 50000;
    private final static int NUM_SERVERS = 4;
    private String lastServer;


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        // TODO: clear dataPath directory
        for (KVServer server : servers.values()) {
            server.clearStorage();
        }
    }

    public void setUpECS() {
        try {
            new LogSetup("logs/testing/test.log", Level.ALL);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ecsThread = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Creating ECS...");
                try {
                    ecs = new ECSClient("localhost", ECS_PORT, false);
                    ecs.run();
                } catch (UnknownHostException e) {
                    System.out.println("Unknown host!");
                }
            }
        });
        ecsThread.start();
    }

    /**
     * Start a new KVServer on a new thread.
     */
    public KVServer setupServer(String address, int port) {
        final KVServer[] server = {null};
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    InetAddress ecs_inet = InetAddress.getByName(ECS_ADDRESS);
                    server[0] = new KVServer(port, 10, "FIFO", address,
                            DATA_PATH , ecs_inet, ECS_PORT, false);
                    server[0].run();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }).start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return server[0];
    }

    @Before
    public void setUp() throws Exception {
        // setup client?
        if (setup) return;
        setup = true;
        setUpECS();
    }

    @After
    public void tearDown() throws Exception {

    }

    /**
     * 1 INIT: start ECS; start a kv_server (kvS1). Are they properly connected?
     * 2 BASIC: start a client; put some key-value pairs to kvS1. Does kvS1 handle all the data?
     * 3 ADD: start more kv servers. Will the ECS update the hash ring correctly and new servers
     *        can find their positions? Will the data be transferred correctly to the right server?
     *        (Check it through PUTs/GETs from the client)
     * 4 REMOVE: shutdown some kv servers. Similar to ADD, Will the hash ring be updated with proper
     *          data transfer?
     * 5 LAST ONE STANDING: leave one kv server (kvS_last; could be any of them) working and shut down
     *          all the other kv servers. Will kvS_last have all the data?
     * 6 REBOOT LAST ONE STANDING: reboot kvS_last; will it still have all the data? (Persistent)
     */


    /**
     * 1 INIT: start ECS; start a kv_server (kvS1). Are they properly connected?
     */
//    @Test
    public void testInit() {
        // ecs already started in setUp()
        // start a kv_server (kvS1)
        KVServer server = setupServer("localhost", SERVER_START_PORT);
        servers.put("localhost:" + SERVER_START_PORT, server);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // Are they properly connected?
        assertNotNull(ecs.getNodes().get("localhost:50000"));

        // TODO: this doesn't work rn but logic is correct. Might switch to no JUnit
    }

    /**
     * 2 BASIC: start a client; put some key-value pairs to kvS1. Does kvS1 handle all the data?
     */
//    @Test
    public void testBasic() {
        client = new KVStore ("localhost", SERVER_START_PORT);
        Exception ex = null;
        try {
            client.connect();
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assertNull(ex);
        // put some key-value pairs to kvS1
        for (int i = 0; i < NUM_KEYS; i++) {
            try {
                client.put("key" + i, "value" + i);
            } catch (Exception e) {
                ex = e;
                e.printStackTrace();
            }
        }

        // Does kvS1 handle all the data?
        KVServer server = servers.get("localhost:" + SERVER_START_PORT);
        for (int i = 0; i < NUM_KEYS; i++) {
            try {
                assertEquals("value" + i, server.getKV("key" + i));
            } catch (Exception e) {
                ex = e;
                e.printStackTrace();
            }
        }
        assertNull(ex);
    }

    /**
     * 3 ADD: start more kv servers. Will the ECS update the hash ring correctly and new servers
     *        can find their positions? Will the data be transferred correctly to the right server?
     *        (Check it through PUTs/GETs from the client)
     */
//    @Test
    public void testAdd() {
        for (int i = 1; i < NUM_SERVERS; i++) {
            int port = SERVER_START_PORT + i;
            servers.put("localhost:" + port,
                    setupServer("localhost", port));
        }

        // Will the ECS update the hash ring correctly and new servers can find their positions?
        // TODO: honestly don't know how to verify hash ring correctness. Just pray to God
        assertEquals(NUM_SERVERS, ecs.getNodes().size());

        // Will the data be transferred correctly to the right server?
        // (Check it through PUTs/GETs from the client)
        client_app = new KVClient();
        client_app.kvstore = this.client;
        Exception ex = null;
        int num_reconnections = 0;
        for (int i = 0; i < NUM_KEYS; i++) {
            try {
                String key = "key" + i;
                String value = "value" + i;
                String response = client_app.handleCommand("put " + key + " " + value);
                if(response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString())) {
                    num_reconnections++;
                    Thread.sleep(1000);
                    response = client_app.handleCommand("put " + key + " " + value);
                }
                assertEquals(IKVMessage.StatusType.PUT_UPDATE.toString(), response);
            } catch (Exception e) {
                e.printStackTrace();
                ex = e;
            }
        }
        assert num_reconnections > 0;
        assertTrue(num_reconnections <= NUM_KEYS);
        assertNull(ex);
    }

    /**
     * 4 REMOVE: shutdown some kv servers. Similar to ADD, Will the hash ring be updated with proper
     *          data transfer?
     */
    public void testRemove() throws Exception {
        // shutdown some kv servers
        Iterator<Map.Entry<String, KVServer>> it = servers.entrySet().iterator();
        while(servers.size() > NUM_SERVERS/2 && it.hasNext()) {
            Map.Entry<String, KVServer> entry = it.next();
            KVServer server = entry.getValue();
            server.kill();
            server.runShutDownHook();
//            try {
//                Thread.sleep(2000);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
            it.remove();
        }

        // Will the hash ring be updated with proper data transfer?
        // check that all the data is still there
        String[] anyServer = servers.entrySet().iterator().next().getKey().split(":");
        // reconnect client to any server
        client_app.handleCommand("connect " + anyServer[0] + " " + anyServer[1]);
        Exception ex = null;
        int num_reconnections = 0;
        for (int i = 0; i < NUM_KEYS; i++) {
            try {
                String key = "key" + i;
                String value = "value" + i;
                String response = client_app.handleCommand("get " + key);
                if(response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString())) {
                    num_reconnections++;
                    Thread.sleep(1000);
                    response = client_app.handleCommand("get " + key);
                }
                assertEquals(GET_SUCCESS.toString(), response);
            } catch (Exception e) {
                e.printStackTrace();
                ex = e;
                fail();
            }
            assertNull(ex);
        }
    }

    /**
     * 5 LAST ONE STANDING: leave one kv server (kvS_last; could be any of them) working and shut down
     *          all the other kv servers. Will kvS_last have all the data?
     */
    public void testLastOneStanding() throws Exception {
        // leave one kv server (kvS_last; could be any of them) working
        Iterator<Map.Entry<String, KVServer>> it = servers.entrySet().iterator();
        while (servers.size() > 1 && it.hasNext()) {
            Map.Entry<String, KVServer> entry = it.next();
            KVServer server = entry.getValue();
            server.kill();
            it.remove();
        }

        // Will kvS_last have all the data?
//        KVClient client_app = new KVClient();
        lastServer = servers.entrySet().iterator().next().getKey();
        // reconnect client to last server
        client_app.handleCommand("connect " + lastServer.split(":")[0] + " " + lastServer.split(":")[1]);
        Exception ex = null;
        int num_reconnections = 0;
        for (int i = 0; i < NUM_KEYS; i++) {
            try {
                String key = "key" + i;
                String value = "value" + i;
                String response = client_app.handleCommand("get " + key);
                if(response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString())) {
                    num_reconnections++;
                    Thread.sleep(1000);
                    response = client_app.handleCommand("get " + key);
                }
                assertEquals(GET_SUCCESS.toString(), response);
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        }

        // kill the last server
        servers.get(lastServer).kill();
        servers.remove(lastServer);
    }

    /**
     * 6 REBOOT LAST ONE STANDING: reboot kvS_last; will it still have all the data? (Persistent)
     */
    public void testRebootLastOneStanding() {
        // reboot kvS_last; will it still have all the data? (Persistent)
        KVServer server = setupServer(lastServer.split(":")[0], Integer.parseInt(lastServer.split(":")[1]));
        servers.put(lastServer, server);
        for (int i = 0; i < NUM_KEYS; i++) {
            try {
                assertEquals("value" + i, server.getKV("key" + i));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void testAll() {
        try {
            testInit();
            testBasic();
            testAdd();
            testRemove();
            testLastOneStanding();
            testRebootLastOneStanding();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
