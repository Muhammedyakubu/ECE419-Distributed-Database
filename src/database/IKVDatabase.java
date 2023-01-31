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
     * @return successful or unsuccessful insertion
     */
    public boolean insertPair(String key, String value);

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
