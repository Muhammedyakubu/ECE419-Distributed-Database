package testing;

import database.KVdatabase;
import junit.framework.TestCase;


public class KVDatabaseTest extends TestCase{

    public void testInsert() {
        KVdatabase db = new KVdatabase();
        String key = "foo";
        String value = "bar";
        boolean expected = true;
        boolean actual = db.insertPair(key, value);
        assertEquals(expected, actual);




    }


    public void testGet() {
        KVdatabase db = new KVdatabase();
        String key = "foo";
        String expected = "bar";
        String actual = db.getValue(key);
        assertEquals(expected, actual);
    }

    public void testDelete() {
        KVdatabase db = new KVdatabase();
        String key = "foo";
        boolean expected = true;
        boolean actual = db.deletePair(key);
        assertEquals(expected, actual);
    }

    public void testUpdate() {
        KVdatabase db = new KVdatabase();
        String key = "foo";
        db.insertPair(key, "bar");
        db.insertPair(key, "yeah");
        String expected = "yeah";
        String actual = db.getValue(key);
        assertEquals(expected, actual);
    }

    public void testClear() {
        KVdatabase db = new KVdatabase();
        String key = "foo";
        String key2 = "foo2";
        db.insertPair(key, "bar");
        db.insertPair(key2, "bar2");

        boolean actual = db.clearStorage();
        boolean expected = true;
        assertEquals(expected, actual);

    }
}
