package testing;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import shared.messages.KVMessage;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class PerformanceTest extends TestCase {

    public static int NUM_GET = 5;
    public static int NUM_PUT = 5;
    public static int NUM_RUNS = 10;
    public static int PORT = 8000;
    public static String SERVER_ADDRESS = "localhost";
    public IKVServer.CacheStrategy strategy = IKVServer.CacheStrategy.valueOf("FIFO");
    public int cacheSize = 40;

    public Thread client;
    public KVServer kvServer;
    public Thread serverThread;

    //stores percentage of gets
    public List<Integer> ratios = new ArrayList<Integer>() {{
        add(10);
        add(20);
        add(50);
        add(80);
        add(90);
    } };

    public List<String> cacheStrategies = new ArrayList<String>() {{
        add("None");
        add("FIFO");
        add("LRU");
    }};

    public class ClientThread implements Runnable {
        private int id;

        public ClientThread(int id) {
            this.id = id;
        }
        @Override
        public void run() {
            KVStore kvClient = new KVStore(SERVER_ADDRESS, PORT);
            try {
                kvClient.connect();
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Put some values
            for (int i = 0; i < NUM_PUT; i++) {
                try {
                    KVMessage response = kvClient.put("client_" + this.id + "_key_" + i, "client_" + this.id + "_value_" + i);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // Get some values
            for (int i = 0; i < NUM_GET; i++) {
                try {
                    KVMessage response = (KVMessage) kvClient.get("client_" + this.id + "_key_" + (int) Math.random()*NUM_PUT);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void setUpServer(IKVServer.CacheStrategy strategy) {
        this.kvServer = new KVServer(PORT, cacheSize, strategy.toString(), false);
        this.kvServer.logger.setLevel(Level.OFF);
        this.serverThread = new Thread(new Runnable() {
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
    public void testPerformance() {
        BasicConfigurator.configure();

        this.client = new Thread();


        ClientThread client = new ClientThread(1);
        Thread clientThread = new Thread(client);
        for (int k = 0; k<cacheStrategies.size(); k++) {
            setUpServer(IKVServer.CacheStrategy.valueOf(cacheStrategies.get(k)));
            for (int i = 0; i < ratios.size(); i++) {
                this.NUM_GET = ratios.get(i);
                this.NUM_PUT = 100 - ratios.get(i);
                final long startTime = System.nanoTime();
                for (int j = 0; j < NUM_RUNS; j++) {
                    clientThread.run();
                    try {
                        clientThread.join();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                final long endTime = System.nanoTime();
                float difference = (endTime - startTime) / 1000000;
                System.out.println(this.NUM_PUT + ":" + this.NUM_GET + " put: get ratio with cache strategy " + cacheStrategies.get(k) +  " runs in " + difference / 10 + "ms");
                System.out.println("Latency is " + difference / (100 * 10) + "ms per request");
                System.out.println("Throughput is " + 1000 / (difference / (100 * 10)) + "request per second");
            }
            kvServer.clearStorage();
            kvServer.close();
        }


        System.out.println("All clients finished.");




    }

}