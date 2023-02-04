package testing;

import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import app_kvServer.KVServer;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class LRUCacheTest extends TestCase{

    private KVStore client;
    private static KVServer server;
    private static Thread serverThread;
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
        server = new KVServer(5001, 3, "LRU", false);
        serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        serverThread.start();

        // give the server some time to start up
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void setUp() {
        setUpServer();
        client = new KVStore("localhost", 5001);
        try {
            client.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void tearDown() {
        client.disconnect();
        server.clearStorage();
    }
    @Test
    public void testPutGetLRUCache(){
        String key = "foo";
        String value = "bar";
        Exception ex = null;
        String value_back = "";
        try {
            client.put(key, value);
            value_back = server.cache.getKV(key);
        }
        catch(Exception e){
            System.out.println(e);
            ex = e;
        }
        assertTrue(ex == null && value.equals(value_back));

    }

    @Test
    public void testEvictionLRUCache(){
        String key1 = "foo1";
        String key2 = "foo2";
        String key3 = "foo3";
        String key4 = "foo4";
        String value = "bar";
        Exception ex = null;
        boolean key_there = false;
        boolean key_there_now = true;
        try {
            server.cache.clear();
            client.put(key1, value);
            client.put(key2, value);
            key_there = server.cache.contains(key2);
            client.put(key3, value);
            client.get(key1);
            client.get(key3);
            client.put(key4, value);
            key_there_now = server.cache.contains(key2); //cache size is three, so key2 should be evicted as LRU
        }
        catch(Exception e){
            System.out.println(e);
            ex = e;
        }
        assertTrue(ex == null && key_there == true && key_there_now == false);

    }

    @Test
    public void testDeleteKeyLRUCache(){
        String key = "foo1";
        String value1 = "bar";
        String value2 = null;
        Exception ex = null;
        boolean key_there = false;
        boolean key_there_now = true;
        try {
            server.cache.clear();
            client.put(key, value1);
            key_there = server.cache.contains(key);
            client.put(key, value2);
            key_there_now = server.cache.contains(key); //cache size is three, so one should be evicted with the fourth key added
        }
        catch(Exception e){
            System.out.println(e);
            ex = e;
        }
        assertTrue(ex == null && key_there == true && key_there_now == false);

    }
}
