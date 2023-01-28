package database;


/**
 * Interface for handling persistent database storage system.
 * Acts independent of caching mechanism, and can be implemented using different
 * storage system architectures
 */
public interface IKVDatabase {

    /**
     * Retrieve the value from storage based on key
     * Does not modify database
     *
     */
    public String getValue(String key);

    /**
     * Inserts a key value pair into storage
     * Returns successful or unsuccessful insertion
     */
    public boolean insertPair(String key, String value);

    /**
     * Permenantly deletes a pair in storage
     * Returns successful or unsuccessful deletion
     */
    public boolean deletePair(String key);

    /**
     * Updates the value stored with an attached key
     * Returns successful or unsuccessful update
     */
    public boolean updatePair(String key, String value);




}
