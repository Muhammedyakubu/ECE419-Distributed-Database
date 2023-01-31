package app_kvServer.cache;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class LRUCache implements Cache{
    private LinkedHashMap<String, String> cache;
    private static int maxCacheSize;

    public LRUCache(int cacheSize) {
        this.maxCacheSize = cacheSize;
        // For the Order attribute, true is passed for the last access order (LRU) and false is passed for the insertion order (FIFO)
        this.cache = new LinkedHashMap<String, String>(cacheSize + 1, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(HashMap.Entry<String, String> eldest) {
                return size() > maxCacheSize;
            }
        };
    }

    /**
     * @param key
     * @return {@code true} if the key is in the cache
     */
    @Override
    public boolean contains(String key) {
        return cache.containsKey(key);
    }

    /**
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    public String getKV(String key) throws Exception {
        return cache.get(key);
    }

    /**
     * @param key
     * @param value
     * @throws Exception
     */
    @Override
    public void putKV(String key, String value) throws Exception {
        cache.put(key, value);
    }

    /**
     * needs not be implemented when using LinkedHashMap
     * @throws Exception
     */
    @Override
    public void evictKV() throws Exception {
        // do nothing
    }

    /**
     * @param key
     */
    @Override
    public void deleteKV(String key) {
        cache.remove(key);
    }

    /**
     *
     */
    @Override
    public void clear() {
        cache.clear();
    }
}
