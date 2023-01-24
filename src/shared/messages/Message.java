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
public class Message implements KVMessage {

    String key;
    String value;
    StatusType status;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    /**
     * Constructor for a message with a key, value, and status
     * @param key
     * @param value
     * @param status
     */
    public Message(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    /**
     * Constructor for a message from a byte array
     * @param bytes
     */
    public Message(byte[] bytes) {
        String msg = new String(addCtrChars(bytes)).trim();
        String[] parts = msg.split(" ");
        this.key = parts[0];
        this.value = parts[1];
        this.status = StatusType.valueOf(parts[2]);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
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

    /**
     * return a string representation of the message
     * @return String
     */
    @Override
    public String toString() {
        return key + " " + value + " " + status.toString();
    }

    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    /**
     * Returns an array of bytes that represent the ASCII coded message content.
     *
     * @return the content of this message as an array of bytes
     * 		in ASCII coding.
     */

    public byte[] toByteArray() {
        byte[] bytes = this.toString().getBytes();
        return addCtrChars(bytes);
    }
}
