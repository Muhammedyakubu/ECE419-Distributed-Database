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

    private static Thread ecsThread;
    private ECSClient ecsClient;
    //private ECSConnection ecsConnect;

    public void setUpECS() {
        ecsThread = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Creating ECS...");
                try {
                    ecsClient = new ECSClient("localhost", 10000);
                } catch (UnknownHostException e) {
                    System.out.println("Unknown host!");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }



    public static boolean available(int port) {
        if (port < 1024 || port > 65535) {
            throw new IllegalArgumentException("Invalid start port: " + port);
        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }

    public void setUpServer() {
        if (setup) return;
        setup = true;

        // check if testsuite server is already running
        // skip logger setup if testsuite server is already running
        boolean testsuiteServerRunning = !available(50004);
        if (testsuiteServerRunning) {
            System.out.println("Testsuite server is already running, skipping logger setup");
        } else {
            System.out.println("Testsuite server is not running, setting up logger");
            try {
                new LogSetup("logs/testing/test.log", Level.ALL);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Creating server...");
        try{
           InetAddress addr = InetAddress.getByName("localhost");
            kvServer = new KVServer(50004, 10, "FIFO", addr, "src/KVStorage/testing" , addr, 10000);
        } catch(Exception e){
           System.out.println("Ugh");
        }
        if (testsuiteServerRunning) {
            System.out.println("Testsuite server is already running, skipping server start");
            return;
        }
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
