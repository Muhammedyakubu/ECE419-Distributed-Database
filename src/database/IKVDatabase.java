package database;


import java.io.IOException;
import java.util.List;

/**
 * Interface for handling persistent database storage system.
 * Acts independent of caching mechanism, and can be implemented using different
 * storage system architectures
 */
public interface IKVDatabase {

    /**
     * Gets a value
     * @param key
     * @return value in string
     */
    public String getValue(String key);

    /**
     * Inserts/updates KV Pair
     * @param key
     * @param value
     * @return {@code true} if the key already exists in the database or if the
     *        key-value pair was successfully deleted, {@code false} otherwise
     * @throws Exception
     *     when there's an error inserting the key-value pair into the database
     */
    public boolean insertPair(String key, String value) throws Exception ;

    /**
     * Permanently deletes a pair in storage
     *
     * @param key
     * @return Successful or unsuccessful deletion
     */
    public boolean deletePair(String key) throws IOException;

    /**
     * Clears all existing KV pairs
     * @return successful or unsuccessful clearance
     */
    public boolean clearStorage();

    /**
     * Get all keys in the current db
     * @return String[] of keys
     */
    public String[] getAllKeys();

    /* Subscription methods */

    /**
     * Get all the client IDs that are subscribed to a key
     * @param key
     * @return List of client IDs
     */
    public List<String> getSubscribers(String key);

    /**
     * Add a client ID to the list of subscribers for a key
     * @param key
     * @param clientID
     */
    public void addSubscriber(String key, String clientID) throws Exception;

    /**
     * Remove a client ID from the list of subscribers for a key
     *
     * @param key
     * @param clientID
     * @return true if the client ID was removed, false if the client ID was not
     *        subscribed to the key
     */
    public boolean removeSubscriber(String key, String clientID) throws Exception;
}
