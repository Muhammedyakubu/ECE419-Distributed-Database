package testing;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvServer.ECSConnection;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import shared.messages.IKVMessage;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

public class ECSInteractionTest extends TestCase {

    private KVStore kvClient;
    private static KVServer kvServer;
    private static Thread serverThread;
    private KVClient client_app;
    private static boolean setup = false;
    private static boolean setup_server = false;

    private static Thread ecsThread;
    private ECSClient ecsClient;
    //private ECSConnection ecsConnect;

    public void setUpECS() {
        if (setup) return;
        setup = true;
        //System.out.println("Creating ECS...");
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
                    ecsClient = new ECSClient("localhost", 10004);
                } catch (UnknownHostException e) {
                    System.out.println("Unknown host!");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
//        try {
//            ecsThread.sleep(2000);
//        } catch (InterruptedException e) {
//            System.out.println("Sleep failed.");
//        }
        ecsThread.start();
    }



//    public static boolean available(int port) {
//        if (port < 1024 || port > 65535) {
//            throw new IllegalArgumentException("Invalid start port: " + port);
//        }
//
//        ServerSocket ss = null;
//        DatagramSocket ds = null;
//        try {
//            ss = new ServerSocket(port);
//            ss.setReuseAddress(true);
//            ds = new DatagramSocket(port);
//            ds.setReuseAddress(true);
//            return true;
//        } catch (IOException e) {
//        } finally {
//            if (ds != null) {
//                ds.close();
//            }
//
//            if (ss != null) {
//                try {
//                    ss.close();
//                } catch (IOException e) {
//                    /* should not be thrown */
//                }
//            }
//        }
//
//        return false;
//    }

    public void setUpServer() {
        if (setup_server) return;
        setup_server = true;
        // check if testsuite server is already running
        // skip logger setup if testsuite server is already running
//        boolean testsuiteServerRunning = !available(50000);
//        if (testsuiteServerRunning) {
//            System.out.println("Testsuite server is already running, skipping logger setup");
//        } else {
//            System.out.println("Testsuite server is not running, setting up logger");
//            try {
//                new LogSetup("logs/testing/test.log", Level.ALL);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }

        System.out.println("Creating server...");
        try{
           InetAddress addr = InetAddress.getByName("localhost");
            kvServer = new KVServer(49995, 10, "FIFO", "localhost",
                    "src/KVStorage" , addr, 10004);
            //kvServer.run();
        } catch(Exception e){
           System.out.println("Ugh");
        }
//        if (testsuiteServerRunning) {
//            System.out.println("Testsuite server is already running, skipping server start");
//            return;
//        }
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
        setUpECS();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            System.out.println("Sleep failed.");
        }
        setUpServer();
        System.out.println("Creating client...");
        kvClient = new KVStore("localhost", 49995);
        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        kvClient.disconnect();
        //kvServer.clearStorage();
        kvServer.close();
        ecsClient.shutdown();
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

        Assert.assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.PUT_SUCCESS);
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

        Assert.assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.PUT_UPDATE
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

        Assert.assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.DELETE_SUCCESS);
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
        Assert.assertTrue(ex == null && response.getStatus() == IKVMessage.StatusType.GET_ERROR);
    }

    @Test
    public void testKeyrangeRequest() {

        String command = "keyrange";
        String response = null;
        Exception ex = null;
        client_app = new KVClient();
        //NEED TO FIX SERVER_STOPPED STATUS
        try {
            client_app.kvstore = this.kvClient;
            response = client_app.handleCommand(command);
        } catch (Exception e) {
            ex = e;
        }
        Assert.assertTrue(ex == null && response.equals(IKVMessage.StatusType.KEYRANGE_SUCCESS.toString()));
    }
}
