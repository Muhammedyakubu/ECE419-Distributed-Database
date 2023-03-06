package testing;

import app_kvServer.KVServer;
import org.junit.Test;
import junit.framework.TestCase;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class KVServerTest extends TestCase{

    private KVServer kvServer;
    private int port;
    private String hostname;

    public void setUp() {
        port = 50000;

        try {
            InetAddress ip = InetAddress.getLocalHost();
            hostname = ip.getHostName();
        }
        catch(UnknownHostException e){
            hostname = "localhost";
        }
        kvServer = new KVServer(port, 10, "FIFO");
    }

    public void tearDown() {
        kvServer.clearStorage();
        kvServer.clearCache();
    }

    /**
     * Can't test this effectively because it differs across machines
     */
//    @Test
//    public void testGetHostname() {
//        assertEquals(hostname, kvServer.getHostname());
//    }

//    @Test
//    public void testValidServerCLEntry() {
//        String[] command_line = {"-p", "5000", "-a", "localhost", "-d", "database/KVStorageTest",
//                                "-l", "logs/KVServer.log", "-ll", "INFO", "-b", "localhost:45000"};
//        String response = kvServer.parseCommandLine(command_line, false);
//        assertEquals(response, "Port: 5000 Address: localhost Datapath: database/KVStorageTest Logpath: " +
//                                        "logs/KVServer.log Loglevel: INFO Bootstrap ECS: localhost:45000");
//    }

    @Test
    public void testNoPortServerCLEntry() {
        String[] command_line = {"-a", "localhost", "-d", "database/KVStorageTest",
                "-l", "logs/KVServer.log", "-ll", "INFO"};
        String response = kvServer.parseCommandLine(command_line, false);
        assertEquals(response, "No port, invalid");
    }

//    @Test
//    public void testInvalidServerCLEntry() {
//        String[] command_line = {"-p", "5000", "-a", "-d", "this/is/a/path", "does/this/work"};
//        String response = kvServer.parseCommandLine(command_line, false);
//        assertEquals(response, "Invalid");
//    }
}
