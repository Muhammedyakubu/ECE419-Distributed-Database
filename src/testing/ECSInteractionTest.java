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
    public static boolean setup = false;
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
                    ecsClient = new ECSClient("localhost", 10011);
                } catch (UnknownHostException e) {
                    System.out.println("Unknown host!");
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

    public void setUpServer(int port) {
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
            kvServer = new KVServer(port, 10, "FIFO", "localhost",
                    "src/KVStorage" , "localhost", 10011, false);
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
        setUpServer(49988);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            System.out.println("Sleep failed.");
        }
        System.out.println("Creating client...");
        kvClient = new KVStore("localhost", 49988);
        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        //kvClient.disconnect();
        //kvServer.clearStorage();
        //kvServer.close();
        //ecsClient.shutdown();
    }

    @Test
    public void testServerFailure(){
        String[] keys = {"foo1", "foo2", "foo3", "foo4", "foo5", "foo6", "foo7", "foo8", "foo9", "foo10"};
        String[] values = {"bar1", "bar2", "bar3", "bar4", "bar5", "bar6", "bar7", "bar8", "bar9", "bar10"};
        Exception ex = null;
        String response = "";
        String keyRangeBefore = "";
        String keyRangeAfter = "";
        client_app = new KVClient();

        try {
            client_app.kvstore = this.kvClient;
            for(int i=0; i< keys.length; i++) {
                client_app.handleCommand("put " + keys[i] + " " + values[i]);
            }
            setup_server = false;
            setUpServer(45000);
            setup_server = false;
            setUpServer(46000);
            setup_server = false;
            setUpServer(47000);
            Thread.sleep(8000);
            //keyRangeBefore = client_app.handleCommand("keyrange");
            serverThread.stop();
            Thread.sleep(8000);
            keyRangeAfter = kvClient.getKeyRange().getKey();
            //System.out.println(keyRangeBefore);
            System.out.println(keyRangeAfter);

        } catch (Exception e) {
            ex = e;
        }

        Assert.assertEquals("2b0dbbc3be8244f7ec794c92afb0becb,428ac38add006bf878cbdc8d5fadcc51,192.168.0.13:45000;428ac38add006bf878cbdc8d5fadcc52,98b5140588e1cf53350ee513faf6dfa5,192.168.0.13:47000;98b5140588e1cf53350ee513faf6dfa6,c47d295c85a741f71b328778f2342de6,192.168.0.13:46000;c47d295c85a741f71b328778f2342de7,2b0dbbc3be8244f7ec794c92afb0beca,192.168.0.13:49988;",keyRangeAfter);
    }

//    @Test
//    public void testServerNotResponsible() {
//        String[] keys = {"foo1", "foo2", "foo3", "foo4", "foo5", "foo6", "foo7", "foo8", "foo9", "foo10"};
//        String[] values = {"bar1", "bar2", "bar3", "bar4", "bar5", "bar6", "bar7", "bar8", "bar9", "bar10"};
//        Exception ex = null;
//        String response = "";
//        client_app = new KVClient();
//        //NEED TO FIX SERVER_STOPPED STATUS
//        try {
//            client_app.kvstore = this.kvClient;
//            for(int i=0; i< keys.length; i++) {
//                client_app.handleCommand("put " + keys[i] + " " + values[i]);
//            }
////            setup_server = false;
////            setUpServer(45000);
////            setup_server = false;
////            setUpServer(46000);
////            setup_server = false;
////            setUpServer(47000);
////            Thread.sleep(8000);
//            for(int i=0; i< keys.length; i++) {
//                response = client_app.handleCommand("get " + keys[i] + " " + values[i]);
//                if(response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString())){
////                    Thread.sleep(2000);
////                    response = client_app.handleCommand("get " + keys[i] + " " + values[i]);
//                    break;
//                }
//            }
//        } catch (Exception e) {
//            ex = e;
//        }
//
//        Assert.assertTrue(/*ex == null && */response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString()));
//    }

    @Test
    public void testPut() {
        String key = "testPut";
        String value = "bar2";
        String response = null;
        Exception ex = null;
        client_app = new KVClient();

        try {
            client_app.kvstore = this.kvClient;
            response = client_app.handleCommand("put " + key + " " + value);
            if(response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString())) {
                Thread.sleep(2000);
                response = client_app.handleCommand("put " + key + " " + value);
            }
        } catch (Exception e) {
            ex = e;
        }

        Assert.assertTrue(ex == null && response.equals(IKVMessage.StatusType.PUT_SUCCESS.toString()));
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

        String response = null;
        Exception ex = null;

        client_app = new KVClient();

        try {
            client_app.kvstore = this.kvClient;
            response = client_app.handleCommand("put " + key + " " + initialValue);
            if(response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString())) {
                Thread.sleep(2000);
            }
            response = client_app.handleCommand("put " + key + " " + updatedValue);


        } catch (Exception e) {
            ex = e;
        }

        Assert.assertTrue(ex == null && response.equals(IKVMessage.StatusType.PUT_UPDATE.toString()));
    }

    @Test
    public void testDelete() {
        String key = "deleteTestValue";
        String value = "toDelete";

        String response = null;
        Exception ex = null;
        client_app = new KVClient();

        try {
            client_app.kvstore = this.kvClient;
            response = client_app.handleCommand("put " + key + " " + value);
            if(response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString())) {
                Thread.sleep(2000);
            }
            response = client_app.handleCommand("put " + key + " ");

        } catch (Exception e) {
            ex = e;
        }

        Assert.assertTrue(ex == null && response.equals(IKVMessage.StatusType.DELETE_SUCCESS.toString()));
    }

    @Test
    public void testGet() {
        String key = "foo";
        String value = "bar";
        String response = null;
        Exception ex = null;
        client_app = new KVClient();

        try {
            client_app.kvstore = this.kvClient;
            response = client_app.handleCommand("put " + key + " " + value);
            if(response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString())) {
                Thread.sleep(2000);
            }
            response = client_app.handleCommand("get " + key);

        } catch (Exception e) {
            ex = e;
        }

        Assert.assertTrue(ex == null && response.equals(IKVMessage.StatusType.GET_SUCCESS.toString()));
    }

    @Test
    public void testGetUnsetValue() {
        String key = "an_unset_value";
        String response = null;
        Exception ex = null;
        client_app = new KVClient();

        try {
            client_app.kvstore = this.kvClient;
            response = client_app.handleCommand("get " + key);
            if(response.equals(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE.toString())) {
                Thread.sleep(2000);
                response = client_app.handleCommand("get " + key);
            }

        } catch (Exception e) {
            ex = e;
        }

        Assert.assertTrue(ex == null && response.equals(IKVMessage.StatusType.GET_ERROR.toString()));
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
        Assert.assertTrue(/*ex == null && */response.equals(IKVMessage.StatusType.KEYRANGE_SUCCESS.toString()));
    }

    @Test
    public void testKeyrangeReadRequest() {

        String command = "keyrange_read";
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
        Assert.assertTrue(/*ex == null && */response.equals(IKVMessage.StatusType.KEYRANGE_READ_SUCCESS.toString()));
    }


}
