package app_kvServer.cache;

public class LRUCache implements Cache{

    public LRUCache(int cacheSize) {

    }

    /**
     * @param key
     * @return
     */
    @Override
    public boolean contains(String key) {
        return false;
    }

    /**
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    public String getKV(String key) throws Exception {
        return null;
    }

    /**
     * @param key
     * @param value
     * @throws Exception
     */
    @Override
    public void putKV(String key, String value) throws Exception {

    }

    /**
     * @throws Exception
     */
    @Override
    public void evictKV() throws Exception {

    }

    /**
     *
     */
    @Override
    public void clearCache() {

    }
}
