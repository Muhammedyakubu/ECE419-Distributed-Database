package testing;

import app_kvServer.KVServer;
import org.junit.Test;
import junit.framework.TestCase;

public class KVServerTest extends TestCase{

    private KVServer kvServer;
    private int port;
    private String hostname;

    public void setUp() {
        port = 50000;
        hostname = "Muhammeds";
        kvServer = new KVServer(port, 10, "FIFO");
    }

    public void tearDown() {
        kvServer.clearStorage();
        kvServer.clearCache();
    }

    /**
     * Can't test this effectively because it differs across machines
     */
    @Test
    public void testGetHostname() {
        assertEquals(hostname, kvServer.getHostname());
    }
}
