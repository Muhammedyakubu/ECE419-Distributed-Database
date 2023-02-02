package database;


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
    public boolean deletePair(String key);

    /**
     * Clears all existing KV pairs
     * @return successful or unsuccessful clearance
     */
    public boolean clearStorage();

}
