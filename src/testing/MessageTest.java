package testing;

import shared.messages.Message;
import shared.messages.KVMessage.StatusType;

import junit.framework.TestCase;
public class MessageTest extends TestCase{

    public void testToString() {
        Message msg = new Message("key", "value", StatusType.GET);
        String expected = "key value GET";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

    public void testToByteArray() {
        Message msg = new Message("key", "value", StatusType.GET);
        byte[] expected = {107, 101, 121, 32, 118, 97, 108, 117, 101, 32, 71, 69, 84, 10, 13};
        byte[] actual = msg.toByteArray();
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    public void testToByteArrayWithError() {
        Message msg = new Message("key", "value", StatusType.GET_ERROR);
        byte[] expected = {107, 101, 121, 32, 118, 97, 108, 117, 101, 32, 71, 69, 84, 95, 69, 82, 82, 79, 82, 10, 13};
        byte[] actual = msg.toByteArray();
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    public void testFromByteArray() {
        byte[] bytes = {107, 101, 121, 32, 118, 97, 108, 117, 101, 32, 71, 69, 84, 10, 13};
        Message msg = new Message(bytes);
        String expected = "key value GET";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

    public void testFromByteArrayWithError() {
        byte[] bytes = {107, 101, 121, 32, 118, 97, 108, 117, 101, 32, 71, 69, 84, 95, 69, 82, 82, 79, 82, 10, 13};
        Message msg = new Message(bytes);
        String expected = "key value GET_ERROR";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

}
