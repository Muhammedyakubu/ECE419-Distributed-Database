package testing;

import org.junit.Test;
import shared.Range;
import shared.messages.KVMetadata;

import java.math.BigInteger;

import junit.framework.TestCase;
import shared.messages.Pair;

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

    @Test
    public void testKVMetadataRemoveLastServer(){
        md.removeServer("localhost",5000);
        assert(md.isEmpty());
    }
    @Test
    public void testKVMetadataAddFirstServer(){
        md.removeServer("localhost",5000);
        md.addServer("localhost",5001);
        BigInteger hash = getHash("localhost:5001");
        BigInteger start = hash.add(BigInteger.ONE);
        String expected = start.toString(16) + "," + hash.toString(16) + "," + "localhost:5001;";
        String actual = md.toString();
        assertEquals(expected, actual);
    }
    @Test
    public void testKVMetadataAddServer(){
        md.removeServer("localhost",5000);
        md.addServer("localhost",5000);
        Pair<String, Range> test = md.addServer("localhost",5200);
        BigInteger hash_one = getHash("localhost:5000");
        BigInteger hash_two = getHash("localhost:5200");
        BigInteger start_one = hash_two.add(BigInteger.ONE);
        BigInteger start_two = hash_one.add(BigInteger.ONE);
        String expected = start_two.toString(16) + "," + hash_two.toString(16) + "," + "localhost:5200;"
                            + start_one.toString(16) + "," + hash_one.toString(16) + "," + "localhost:5000;";
        String actual = md.toString();
        assertEquals(expected, actual);
        //assertEquals(expected, actual);
    }

    //WHEN DELETING LAST, THE OTHER GETS DELETED
    @Test
    public void testKVMetadataRemoveServer(){
        md.removeServer("localhost",5000);
        md.addServer("localhost",5000);
        md.addServer("localhost",5200);
        Pair<String, Range> test = md.removeServer("localhost",5000);
        BigInteger hash = getHash("localhost:5200");
        BigInteger start = hash.add(BigInteger.ONE);
        String expected = start.toString(16) + "," + hash.toString(16) + "," + "localhost:5200;";
        String actual = md.toString();
        assertEquals(expected, actual);
        //assertEquals(expected, actual);
    }

    @Test
    public void testKVMetadataGetNthSuccessorOneServer(){
        md.removeServer("localhost",5000);
        md.addServer("localhost",5000);

        //5100, 5500
        System.out.println(md.toString());
        assertEquals("localhost:5000",md.getNthSuccessor("localhost:5000", 3).getFirst());
    }

    @Test
    public void testKVMetadataGetNthSuccessor(){
        md.removeServer("localhost",5000);
        md.addServer("localhost",5000);
        md.addServer("localhost",5100);
        md.addServer("localhost",5200);
        md.addServer("localhost",5300);
        md.addServer("localhost",5400);
        md.addServer("localhost",5500);

        //5100, 5500
        System.out.println(md.toString());
        assertEquals("localhost:5500",md.getNthSuccessor("localhost:5100", 3).getFirst());
    }

    @Test
    public void testKVMetadataGetNthPredecessor(){
        md.removeServer("localhost",5000);
        md.addServer("localhost",5000);
        md.addServer("localhost",5100);
        md.addServer("localhost",5200);
        md.addServer("localhost",5300);
        md.addServer("localhost",5400);
        md.addServer("localhost",5500);

        //5100, 5500
        System.out.println(md.toString());
        assertEquals("localhost:5400",md.getNthSuccessor("localhost:5300", -3).getFirst());
    }

    public void testKVMetadataGet6thPredecessor(){
        md.removeServer("localhost",5000);
        md.addServer("localhost",5000);
        md.addServer("localhost",5100);
        md.addServer("localhost",5200);

        //5100, 5500
        System.out.println(md.toString());
        assertEquals("localhost:5100",md.getNthSuccessor("localhost:5100", -6).getFirst());
    }

    public void testKVMetadataGet7thSuccessor(){
        md.removeServer("localhost",5000);
        md.addServer("localhost",5000);
        md.addServer("localhost",5100);
        md.addServer("localhost",5200);

        //5100, 5500
        System.out.println(md.toString());
        assertEquals("localhost:5000",md.getNthSuccessor("localhost:5100", 7).getFirst());
    }

    @Test
    public void testKVMetadataGetKeyRangeRead(){

        md.removeServer("localhost",5000);
        md.addServer("localhost",5000);
        md.addServer("localhost",5100);
        md.addServer("localhost",5200);
        md.addServer("localhost",5300);
        md.addServer("localhost",5400);

        System.out.println(md.toString());
        System.out.println(md.toKeyRangeReadString());
        assertEquals("341e97d7bf9a69b130a0db09a8c87cfe,cc9b4f9d4d774ddcec786208bfa0aefd,localhost:5300;" +
                "ab2bd578b78979e9d15ce0bfade24988,ecadfd0ee12447ea5a63a5705822355a,localhost:5200;" +
                "b18c9873dcbbe400e116c6e3d9644376,341e97d7bf9a69b130a0db09a8c87cfd,localhost:5400;" +
                "cc9b4f9d4d774ddcec786208bfa0aefe,ab2bd578b78979e9d15ce0bfade24987,localhost:5100;" +
                "ecadfd0ee12447ea5a63a5705822355b,b18c9873dcbbe400e116c6e3d9644375,localhost:5000;",md.toKeyRangeReadString());

    }

}
