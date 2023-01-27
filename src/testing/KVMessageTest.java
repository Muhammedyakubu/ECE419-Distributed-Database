package testing;

import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;

import junit.framework.TestCase;
public class KVMessageTest extends TestCase{

    public void testToString() {
        KVMessage msg = new KVMessage(StatusType.GET, "key", "value");
        String expected = "GET key value";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

    public void testToByteArray() {
        KVMessage msg = new KVMessage(StatusType.GET, "key", "value");
        byte[] expected = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 10, 13};
        byte[] actual = msg.toByteArray();
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    public void testToByteArrayWithError() {
        KVMessage msg = new KVMessage(StatusType.GET_ERROR, "key", "value");
        byte[] expected = {71, 69, 84, 95, 69, 82, 82, 79, 82, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 10, 13};
        byte[] actual = msg.toByteArray();
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    public void testFromByteArray() {
        byte[] bytes = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 10, 13};
        KVMessage msg = new KVMessage(bytes);
        String expected = "GET key value";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

    public void testFromByteArrayWithError() {
        byte[] bytes = {71, 69, 84, 95, 69, 82, 82, 79, 82, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 10, 13};
        KVMessage msg = new KVMessage(bytes);
        String expected = "GET_ERROR key value";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

}
