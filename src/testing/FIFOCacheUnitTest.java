package testing;

import app_kvServer.cache.FIFOCache;
import junit.framework.TestCase;

public class FIFOCacheUnitTest extends TestCase{

    public FIFOCache cache = new FIFOCache(3);

    public void testPutGetFIFOCacheUNIT(){
        String key = "foo";
        String value = "bar";
        Exception ex = null;
        String value_back = "";
        try {
            cache.putKV(key, value);
            value_back = cache.getKV(key);
        }
        catch(Exception e){
            System.out.println(e);
            ex = e;
        }
        assertTrue(ex == null && value.equals(value_back));

    }

    public void testEvictionFIFOCacheUNIT(){

        //Was failing in All Tests without this line
        FIFOCache cache = new FIFOCache(3);

        String key1 = "foo1";
        String key2 = "foo2";
        String key3 = "foo3";
        String key4 = "foo4";
        String value = "bar";
        Exception ex = null;
        boolean key_there = false;
        boolean key_there_now = true;
        try {
            cache.clear();
            cache.putKV(key1, value);
            key_there = cache.contains(key1);
            cache.putKV(key2, value);
            cache.putKV(key3, value);
            cache.putKV(key4, value);
            key_there_now = cache.contains(key1); //cache size is three, so one should be evicted with the fourth key added
        }
        catch(Exception e){
            System.out.println(e);
            ex = e;
        }
        assertTrue(ex == null && key_there == true && key_there_now == false);

    }


    public void testDeleteKeyFIFOCacheUNIT(){
        String key = "foo1";
        String value = "bar";
        Exception ex = null;
        boolean key_there = false;
        boolean key_there_now = true;
        try {
            cache.clear();
            cache.putKV(key, value);
            key_there = cache.contains(key);
            cache.deleteKV(key);
            key_there_now = cache.contains(key); //cache size is three, so one should be evicted with the fourth key added
        }
        catch(Exception e){
            System.out.println(e);
            ex = e;
        }
        assertTrue(ex == null && key_there == true && key_there_now == false);

    }
}
