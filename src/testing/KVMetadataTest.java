package testing;

import org.junit.Test;
import shared.Range;
import shared.messages.KVMetadata;

import java.math.BigInteger;

import junit.framework.TestCase;

import static shared.MD5.getHash;

public class KVMetadataTest extends TestCase{

    Range range = new Range(BigInteger.valueOf(0), BigInteger.valueOf(5));
    KVMetadata md = new KVMetadata(5000, "localhost", range);
    @Test
    public void testKVMetadataToString(){
        String expected = "0,5,localhost:5000;";
        String actual = md.toString();
        assertEquals(expected, actual);
    }
    @Test
    public void testKVMetadataAddServerWStartEndpoint(){
        md.addServer("localhost:5001", BigInteger.valueOf(6), BigInteger.valueOf(10));
        String expected = "0,5,localhost:5000;6,a,localhost:5001;";
        String actual = md.toString();
        assertEquals(expected, actual);
    }
    @Test
    public void testKVMetadataToMetadata(){
        md.addServer("localhost:5001", BigInteger.valueOf(6), BigInteger.valueOf(10));
        String in_between = md.toString();
        KVMetadata test_md = new KVMetadata(in_between);
        String expected = md.toString();
        String actual = test_md.toString();
        assertEquals(expected, actual);
    }
    @Test
    public void testKVMetadataFindServer(){
        BigInteger hash = getHash("hi");
        md.addServer("localhost:5001", BigInteger.valueOf(10), hash);
        String expected = "localhost:5001";
        String actual = md.findServer("hi");
        assertEquals(expected, actual);
    }
}
