package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import shared.messages.IKVMessage;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ServerSetupTeardownTest extends TestCase {

    public static boolean setup = false;
    public static boolean setup_server = false;
    private ECSClient ecsClient;
    private KVServer[] kvServer = new KVServer[100];
    private static Thread ecsThread;
    private static Thread[] serverThread = new Thread[100];
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
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        ecsThread.start();
    }

    public void setUpServer(int port, int index) {
        if (setup_server) return;
        setup_server = true;

        System.out.println("Creating server...");
        try{
            InetAddress addr = InetAddress.getByName("localhost");
            kvServer[index] = new KVServer(port, 10, "FIFO", "localhost",
                    "src/KVStorage" , addr, 10011, false);
            //kvServer.run();
        } catch(Exception e){
            System.out.println("Ugh");
        }

        serverThread[index] = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    kvServer[index].run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        serverThread[index].start();
        //return kvServer[index];
    }

    public void testSetupTeardown10(){
        setUpECS();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            System.out.println("Sleep failed.");
        }
        final long startTime = System.nanoTime();

        for(int i = 0; i< 10; i++){
            setup_server = false;
            setUpServer(40000+i,i);
            try {
                Thread.sleep(12);
            } catch (InterruptedException e) {
                System.out.println("Sleep failed.");
            }
            //System.out.println("DONE");
        }
        for(int i = 0; i< 10; i++){
            while(kvServer[i].currStatus != IKVMessage.ServerState.ACTIVE){
                //SPIN
            }
        }
        final long endTime = System.nanoTime();
        float difference = (endTime - startTime) / 1000000;
        System.out.println(difference);

        final long startTeardown = System.nanoTime();

        for(int i = 0; i< 10; i++){
            kvServer[i].close();
            try {
                Thread.sleep(11);
            } catch (InterruptedException e) {
                System.out.println("Sleep failed.");
            }
            System.out.println("DONE");
        }

//        try {
//                Thread.sleep(1500);
//            } catch (InterruptedException e) {
//                System.out.println("Sleep failed.");
//            }

        final long endTeardown = System.nanoTime();
        float difference2 = (endTeardown - startTeardown) / 1000000;
        System.out.println(difference2);

    }

    public void testSetupTeardown100(){
        setUpECS();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            System.out.println("Sleep failed.");
        }
        final long startTime = System.nanoTime();

        for(int i = 0; i< 100; i++){
            setup_server = false;
            setUpServer(40000+i,i);
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                System.out.println("Sleep failed.");
            }
            //System.out.println("DONE");
        }
        for(int i = 0; i< 100; i++){
            while(kvServer[i].currStatus != IKVMessage.ServerState.ACTIVE){
                //SPIN
            }
        }
        final long endTime = System.nanoTime();
        float difference = (endTime - startTime) / 1000000;
        System.out.println(difference);

        final long startTeardown = System.nanoTime();

        for(int i = 0; i< 100; i++){
            kvServer[i].close();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Sleep failed.");
            }
            System.out.println("DONE");
        }

//        try {
//                Thread.sleep(1500);
//            } catch (InterruptedException e) {
//                System.out.println("Sleep failed.");
//            }

        final long endTeardown = System.nanoTime();
        float difference2 = (endTeardown - startTeardown) / 1000000;
        System.out.println(difference2);

    }

    public void testSetupTeardown1(){
        setUpECS();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            System.out.println("Sleep failed.");
        }
        final long startTime = System.nanoTime();

        for(int i = 0; i< 1; i++){
            setup_server = false;
            setUpServer(40000+i,i);
            //System.out.println("DONE");
        }

        try {
            Thread.sleep(35);
        } catch (InterruptedException e) {
            System.out.println("Sleep failed.");
        }
        for(int i = 0; i< 1; i++){
            while(kvServer[i].currStatus != IKVMessage.ServerState.ACTIVE){
                //SPIN
            }
        }
        final long endTime = System.nanoTime();
        float difference = (endTime - startTime) / 1000000;
        System.out.println(difference);

        final long startTeardown = System.nanoTime();

        for(int i = 0; i< 1; i++){
            kvServer[i].close();
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                System.out.println("Sleep failed.");
//            }
            System.out.println("DONE");
        }

//        try {
//                Thread.sleep(1500);
//            } catch (InterruptedException e) {
//                System.out.println("Sleep failed.");
//            }

        final long endTeardown = System.nanoTime();
        float difference2 = (endTeardown - startTeardown) / 1000000;
        System.out.println(difference2);

    }

}
