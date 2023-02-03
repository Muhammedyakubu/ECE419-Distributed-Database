package shared.messages;


/**
 * Message class for KV messages
 * The same class is used for both client and server messages
 * The message is a StatusType of the form "key value status"
 *
 * Messages from the client use the status types:
 * GET, PUT
 *
 * Messages from the server use the status types:
 * GET_ERROR, GET_SUCCESS, PUT_SUCCESS, PUT_UPDATE, PUT_ERROR, DELETE_SUCCESS, DELETE_ERROR
 */
public class KVMessage implements IKVMessage {

    StatusType status;
    String key;
    String value;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    private static final String DELIMITER = " ";

    /**
     * Constructor for a message with a key, value, and status
     *
     * @param status
     * @param key
     * @param value
     */
    public KVMessage(StatusType status, String key, String value) {
        this.status = status;
        this.key = key;
        this.value = value;
    }

    /**
     * Constructor for a message from a byte array
     * @param bytes
     */
    public KVMessage(byte[] bytes) {
        String msg = new String(rmvCtrChars(bytes));
        String[] parts = decode(msg);
        try {
            this.status = StatusType.valueOf(parts[0].strip().toUpperCase());
            this.key = parts[1].replace("\n", ""); // key won't contain spaces or control characters
            this.value = (parts[2].equals("") || parts[2].equals("null")) ? null : parts[2];
        } catch (Exception e) {
            System.out.println("Error decoding message: '" + msg + "'");
            this.status = StatusType.FAILED;
            this.key = msg;
            this.value = null;
        }
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    /**
     * Set the status of the message (This is used by the server)
     * @param status
     */
    public void setStatus(StatusType status) {
        this.status = status;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    /**
     * Set the value of the message (This is used by the server)
     * @param value
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * return a string representation of the message
     * @return String
     */
    @Override
    public String toString() {
        return status.toString() + " " + key + " " + value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof KVMessage) {
            KVMessage msg = (KVMessage) obj;
            return msg.status.equals(this.status) && msg.key.equals(this.key) && msg.value.equals(this.value);
        }
        return false;
    }

    /**
     * Encode the message into a single {@code DELIMITER} separated string for transmission
     * @return String of the form "statusDELIMITERkeyDELIMITERvalue"
     */
    public String encode() {
    	return status.toString() + DELIMITER + key + DELIMITER + value;
    }

    /**
     * Decode a message from a string into an array of strings
     * @param msg
     * @return String array of the form {status, key, value}
     */
    public String[] decode(String msg) {
        String[] arr = msg.split(DELIMITER, 3);
        if (arr.length == 2) {
            return new String[]{arr[0], arr[1], ""};
        }
    	return arr;
    }

    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{RETURN, LINE_FEED};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    public static byte[] rmvCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{RETURN, LINE_FEED};
        if (bytes[bytes.length - 1] == ctrBytes[1] && bytes[bytes.length - 2] == ctrBytes[0]) {
            byte[] tmp = new byte[bytes.length - ctrBytes.length];
            System.arraycopy(bytes, 0, tmp, 0, bytes.length - ctrBytes.length);
            return tmp;
        }
        return bytes;
    }

    /**
     * Returns an array of bytes that represent the ASCII coded message content.
     *
     * @return the content of this message as an array of bytes
     * 		in ASCII coding.
     */

    public byte[] toByteArray() {
        return addCtrChars(this.encode().getBytes());
    }
}
