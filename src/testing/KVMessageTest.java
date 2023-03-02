package testing;

import app_kvECS.ECSClient;
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

    public void testEncode() {
        KVMessage msg = new KVMessage(StatusType.GET, "key", "value");
        String expected = "GET key value";
        String actual = msg.encode();
        assertEquals(expected, actual);
    }

    public void testToByteArray() {
        KVMessage msg = new KVMessage(StatusType.GET, "key", "value");
        byte[] expected = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 13, 10};
        byte[] actual = msg.toByteArray();
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    public void testRmvCtrChars() {
        byte[] bytes = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 13, 10};
        byte[] expected = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101};
        byte[] actual = KVMessage.rmvCtrChars(bytes);
        printByteArray(expected, actual);
        assertEquals(expected.length, actual.length);
    }

    public void testRmvCtrCharsNoCtrChars() {
        byte[] bytes = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101};
        byte[] expected = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101};
        byte[] actual = KVMessage.rmvCtrChars(bytes);
        printByteArray(expected, actual);
        assertEquals(expected.length, actual.length);
    }

    public void testToByteArrayWithError() {
        KVMessage msg = new KVMessage(StatusType.GET_ERROR, "key", "value");
        byte[] expected = {71, 69, 84, 95, 69, 82, 82, 79, 82, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 13, 10};
        byte[] actual = msg.toByteArray();
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    public void testFromByteArray() {
        byte[] bytes = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 13, 10};
        KVMessage msg = new KVMessage(bytes);
        String expected = "GET key value";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

    public void testFromByteArrayWithError() {
        byte[] bytes = {71, 69, 84, 95, 69, 82, 82, 79, 82, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 13, 10};
        KVMessage msg = new KVMessage(bytes);
        String expected = "GET_ERROR key value";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

    public void testFromByteArrayWithEmptyValue() {
        byte[] bytes = {71, 69, 84, 32, 107, 101, 121, 32, 13, 10};
        KVMessage msg = new KVMessage(bytes);
        String expected = "GET key null";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

    public void testFromByteArrayWithEmptyValueAndNoSpace(){
        byte[] bytes = {71, 69, 84, 32, 107, 101, 121, 13, 10};
        KVMessage msg = new KVMessage(bytes);
        String expected = "GET key null";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

    public void testToByteArrayWithLineFeedInValue() {
        KVMessage msg = new KVMessage(StatusType.GET, "key", "value\n");
        byte[] expected = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 10, 13, 10};
        byte[] actual = msg.toByteArray();
        printByteArray(expected, actual);
        assertEquals(expected.length, actual.length);
    }

    private void printByteArray(byte[] expected, byte[] actual) {
        for (byte b : actual) {
            System.out.print(b + " ");
        }
        System.out.println();
        for (byte b : expected) {
            System.out.print(b + " ");
        }
        System.out.println();
    }

    public void testFromByteArrayWithLineFeedInValue() {
        byte[] bytes = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 10, 13, 10};
        KVMessage msg = new KVMessage(bytes);
        String expected = "GET key value\n";
        String actual = msg.toString();
        assertEquals(expected, actual);
    }

    public void testEquals() {
        KVMessage msg = new KVMessage(StatusType.GET, "key", "value");
        KVMessage msg2 = new KVMessage(StatusType.GET, "key", "value");
        assertEquals(msg, msg2);
    }

    public void testValueWithSpace() {
        KVMessage msg = new KVMessage(StatusType.GET, "key", "value value");
        byte[] expected = {71, 69, 84, 32, 107, 101, 121, 32, 118, 97, 108, 117, 101, 32, 118, 97, 108, 117, 101, 13, 10};
        byte[] actual = msg.toByteArray();
        printByteArray(expected, actual);
        assertEquals(new KVMessage(expected), new KVMessage(actual));
        assertEquals(expected.length, actual.length);
    }

    public void testParseKeyrange() {
        Exception e = null;
        KVMessage msg = new KVMessage(StatusType.FAILED, "", "");
        try {
            msg = new KVMessage((StatusType.KEYRANGE.toString().toLowerCase() + "\r\n").getBytes());
        } catch (Exception ex) {
            e = ex;
            fail("Should not throw exception");
        }
        assertEquals(StatusType.KEYRANGE, msg.getStatus());
        assertNull(e);
    }
}
