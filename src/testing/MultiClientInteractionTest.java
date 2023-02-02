package testing;

import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.util.List;

public class MultiClientInteractionTest extends TestCase {

    public static int NUM_CLIENTS = 10;
    public static int NUM_GET = 5;
    public static int NUM_PUT = 5;
    public static int PORT = 50000;
    public static String SERVER_ADDRESS = "localhost";

    public List<Thread> clients;
    public KVServer kvServer;
    public Thread serverThread;

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
                    assertTrue(
                            response.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS) ||
                                    response.getStatus().equals(KVMessage.StatusType.PUT_UPDATE));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // Get some values
            for (int i = 0; i < NUM_GET; i++) {
                try {
                    KVMessage response = (KVMessage) kvClient.get("client_" + this.id + "_key_" + i);
                    assertEquals(response.getStatus(), KVMessage.StatusType.GET_SUCCESS);
                    assertEquals(response.getValue(), "client_" + this.id + "_value_" + i);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Client " + this.id + " finished.");
        }
    }

    public void setUpServer() {
        this.kvServer = new KVServer(PORT, 10, "None", false);
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
    public void testMultiClientInteraction() {
        BasicConfigurator.configure();
        this.clients = new java.util.ArrayList<Thread>();
        setUpServer();

        for (int i = 0; i < NUM_CLIENTS; i++) {
            ClientThread client = new ClientThread(i);
            Thread clientThread = new Thread(client);
            clients.add(clientThread);
            clientThread.start();
        }

        System.out.println("Waiting for clients to finish...");

        for(Thread client : clients) {
            try {
                client.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("All clients finished.");

        kvServer.clearStorage();
//        kvServer.close();



    }
}
