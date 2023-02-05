package app_kvServer.cache;

/**
 * The interface for an in-memory cache of key-value pairs that have been
 * retrieved from the storage server in the current session.
 */
public interface Cache {
    /**
     * Check if key is in cache.
     * NOTE: does not modify any other properties
     * @return  true if key in cache, false otherwise
     */
    public boolean contains(String key);

    /**
     * Get the value associated with the key
     * @param key key of the key-value pair to be retrieved
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into cache
     * @param key key of the key-value pair to be put
     * @param value value of the key-value pair to be put
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Evict a key-value pair from cache, based on the cache strategy
     * @throws Exception
     *     when key not in the key range of the server
     *     when cache is empty
     */
    public void evictKV() throws Exception;

    /**
     * Delete a key-value pair from cache
     * @param key  key of the key-value pair to be deleted
     */
    public void deleteKV(String key);

    /**
     * Clear the cache
     */
    public void clear();
}
