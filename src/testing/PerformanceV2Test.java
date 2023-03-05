package testing;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import shared.messages.IKVMessage;
import shared.messages.KVMessage;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceV2Test extends TestCase {

    private List <Thread> serverThreads = new ArrayList<>(100);
    private HashMap<Integer, KVClient> clients = new HashMap<>();
    private HashMap<Integer, KVServer> servers = new HashMap<>();
    //private List<KVStore> clients = new ArrayList<>(100);
    //private List<KVServer> servers = new ArrayList<>(100);
    private ECSClient ecsClient;
    static final int numServers = 2;
    static final int numClients = 2;
    static final int serverStartPort = 16000;
    static final int ecsPort = 20000;
    static final int NUM_PUT = 50;
    static final int NUM_GET = 50;
    ExecutorService taskExecutor;
    long difference = 0;
    private final String ENRON_SET = "/Users/jasnoorguliani/maildir";
    private File[] dirList;


    public void setUpECS(final int port) {
        //System.out.println("Creating ECS...");
        /*try {
            new LogSetup("logs/testing/test.log", Level.ALL);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Creating ECS...");
                try {
                    ecsClient = new ECSClient("localhost", port, false);
                } catch (UnknownHostException e) {
                    System.out.println("Unknown host!");
                }
                System.out.println("ECS Initialized");
                ecsClient.run();
            }
        }).start();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            System.out.println("Sleep failed.");
        }


    }
    private class clientProcess implements Runnable {
        private int id;
        private String address;
        private int port;
        LinkedList<AtomicInteger> putCounts = new LinkedList<AtomicInteger>();
        LinkedList<AtomicInteger> getCounts = new LinkedList<AtomicInteger>();

        clientProcess(int id, String address, int port){
            this.id = id;
            this.address = address;
            this.port = port;
            for (int i = 0; i <100; i++){
                this.putCounts.add(new AtomicInteger(0));
                this.getCounts.add(new AtomicInteger(0));
            }
        }
        public void run() {
            clients.put(id, new KVClient());
            File directory = dirList[id];

            try {
                String connectCommand = "connect " + address +" "+port;
                clients.get(id).handleCommand(connectCommand);
            } catch (Exception e) {
                e.printStackTrace();
            }

            //put some values
            putValues(clients.get(id), id, directory, NUM_PUT);
            getValues(clients.get(id), id, directory, NUM_GET);
            //getValues(clients.get(id), id, directory);


            /*
            for (int i = 0; i < NUM_PUT; i++) {
                try {
                    String putCommand = "put client_" + id + "_key_" + i + " client_" + id + "_value_" + i;
                    clients.get(id).handleCommand(putCommand);
                    //KVMessage response = clients.get(id).put("client_" + id + "_key_" + i, "client_" + id + "_value_" + i);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // Get some values
            for (int i = 0; i < NUM_GET; i++) {
                try {
                    String getCommand = "get client_" + id + "_key_" + (int) (Math.random() * NUM_PUT);
                    clients.get(id).handleCommand(getCommand);
                    //KVMessage response = (KVMessage) clients.get(id).get("client_" + id + "_key_" + (int) (Math.random() * NUM_PUT));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }*/

        }
        private void putValues(KVClient client, int id, File dir, int maxCount){
            if (dir.isDirectory()) {
                for (File temp : dir.listFiles()) {
                    if (this.putCounts.get(id).get() == maxCount){
                        return;
                    }
                    if (temp.isDirectory()) {
                        putValues(client, id, temp, maxCount);
                    } else {
                        try {
                            String value = new String(Files.readAllBytes(temp.toPath()));
                            String key = temp.getPath();
                            key = key.substring(dirList[id].toString().length());
                            key = key.replace("/", "");
                            String command = "put "+key + " "+value;
                            client.handleCommand(command);
                            this.putCounts.get(id).incrementAndGet();
                        } catch (Exception e) {
                            System.out.println("Puts failed");
                        }
                    }
                }
            }
        }


        private void getValues(KVClient client, int id, File dir, int maxCount){
            if (dir.isDirectory()) {
                for (File temp : dir.listFiles()) {
                    if (this.getCounts.get(id).get() == maxCount){
                        return;
                    }
                    if (temp.isDirectory()) {
                        getValues(client, id, temp, maxCount);
                    } else {
                        try {
                            String value = new String(Files.readAllBytes(temp.toPath()));
                            String key = temp.getPath();
                            key = key.substring(dirList[id].toString().length());
                            key = key.replace("/", "");
                            String command = "get "+key;
                            client.handleCommand(command);
                            this.getCounts.get(id).incrementAndGet();
                        } catch (Exception e) {
                            System.out.println("Gets failed");
                        }
                    }
                }
            }
        }


    }


    public void setUpServer(int port, int ecs_port, final int index) {

        System.out.println("Creating server...");
        try{
            InetAddress addr = InetAddress.getByName("localhost");
            servers.put(index, new KVServer(port, 10, "FIFO", "localhost",
                    "src/KVStorage", addr, ecs_port, false));
            //kvServer.run();
        } catch(Exception e) {
            ;
        }

        serverThreads.add(index, new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    servers.get(index).run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }));
        serverThreads.get(index).start();
        //wait for the server to activate before continuing
        while (servers.get(index).currStatus != IKVMessage.ServerState.ACTIVE){
            try {
                Thread.sleep(0);
            } catch (InterruptedException e) {
                System.out.println("Sleep interrupted");
            }
        }
    }
    class ShutdownPrint extends Thread {
        public void run() {
            System.out.println("Total Runtime for "+numServers + " servers and "+numClients +
                    " clients with "+(float)NUM_PUT/NUM_GET+" put to get ratio (100 total per client): " + difference + " ms");

        }
    }

    private void loadEnron(int size){
        File file = new File(ENRON_SET);
        dirList = file.listFiles();
        dirList = Arrays.copyOfRange(dirList, 0 , size);

    }


    public void testPerformance() {
        BasicConfigurator.configure();
        taskExecutor = Executors.newFixedThreadPool(numClients);
        Runtime current = Runtime.getRuntime();
        current.addShutdownHook(new ShutdownPrint());
        loadEnron(numClients);

        setUpECS(ecsPort);
        //setup each server and wait for activation
        for (int server = 0; server < numServers; server++) {
            setUpServer(serverStartPort + server, ecsPort, server);
        }
        final long startTime = System.nanoTime();
        //add each client to an executor pool
        for (int client = 0; client < numClients; client++){
            taskExecutor.execute(new clientProcess(client, "localhost", client%numServers + serverStartPort));
        }
        taskExecutor.shutdown(); //close the executor pool and wait for termination
        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.out.println("Failed executing all threads");
        }
        final long endTime = System.nanoTime();

        difference = (endTime - startTime) / 1000000;

        for (int server = 0; server < numServers; server++) {
            servers.get(server).close();
        }



    }




}
