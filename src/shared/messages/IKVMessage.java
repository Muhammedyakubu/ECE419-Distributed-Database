package shared.messages;

public class IKVMessage implements KVMessage {

    String key;
    String value;
    StatusType status;

    public IKVMessage(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
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

    @Override
    public StatusType getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "KVMessageImpl [key=" + key + ", value=" + value + ", status=" + status + "]";
    }

    /**
     * return a string representation of the message
     * @return String
     */
    public String encode() {
        return key + " " + value + " " + status;
    }

    /**
     * decode a string representation of the message
     * @return KVMessageImpl
     */
    public IKVMessage decode(String message) {
        String[] parts = message.split(" ");
        String key = parts[0];
        String value = parts[1];
        StatusType status = StatusType.valueOf(parts[2]);
        return new IKVMessage(key, value, status);
    }
}
