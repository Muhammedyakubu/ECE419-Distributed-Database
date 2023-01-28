package database;


/**
 * Class for manipulating key-value store database using a simple key to file mapping
 * for each pair.
 */
public class KVdatabase implements IKVDatabase{

    @Override
    public String getValue(String key) {
        return null;
    }

    @Override
    public boolean insertPair(String key, String value){
        return false;
    }

    @Override
    public boolean deletePair(String key) {
        return false;
    }

    @Override
    public boolean updatePair(String key, String value) {
        return false;
    }

}
