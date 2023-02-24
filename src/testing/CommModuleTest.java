//package testing;
//
//import junit.framework.TestCase;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import server.Server;
//import shared.comms.CommModule;
//import shared.messages.IKVMessage;
//import shared.messages.KVMessage;
//
//import java.io.IOException;
//import java.net.Socket;
//
//public class CommModuleTest extends TestCase {
//
//    private static Thread serverThread;
//    private static boolean setup = false;
//    private Socket socket;
//    private final int PORT = 50005;
//
//    public void setUpEchoServer() {
//        if (setup) return;
//
//        serverThread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    new Server(PORT).start();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//    }
//
//    @Before
//    public void setUp() {
//        setUpEchoServer();
//        try {
//            socket = new Socket("localhost", PORT);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void testSendWithReceive() throws IOException  {
//        KVMessage msg = new KVMessage(IKVMessage.StatusType.GET, "Hello", null);
//        CommModule.sendMessage(msg, socket);
//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        KVMessage response = CommModule.receiveMessage(socket);
//
//        System.out.println(msg);
//        System.out.println(response);
//
//        assertEquals(msg, response);
//    }
//}
