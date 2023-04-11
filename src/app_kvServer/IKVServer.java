package app_kvServer;

import java.util.List;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    /**
     * Get the port number of the server
     * @return  port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * @return  hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @return true if key exist(ed) in storage, false otherwise
     * @throws Exception
     *      when key not in the key range of the server
     */
    public boolean putKV(String key, String value, boolean withSub) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

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
    public void addSubscriber(String key, String clientID);

    /**
     * Remove a client ID from the list of subscribers for a key
     *
     * @param key
     * @param clientID
     * @return true if the client ID was removed, false if the client ID was not
     *         subscribed to the key
     */
    public boolean removeSubscriber(String key, String clientID);
}
