package app_kvServer.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class FIFOCache implements Cache{
    private Map<String, String> cache;
    private static int maxCacheSize;

    public FIFOCache(int cacheSize) {
        this.maxCacheSize = cacheSize;
        // For the Order attribute, true is passed for the last access order (LRU) and false is passed for the insertion order (FIFO)
        this.cache = Collections.synchronizedMap(new LinkedHashMap<String, String>(cacheSize + 1, 0.75f, false) {
            private static final long serialVersionUID = 12345L; // use something random or just suppress the warning
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > maxCacheSize; // how many entries you want to keep
            }
        });
    }

    /**
     * @param key
     * @return
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
