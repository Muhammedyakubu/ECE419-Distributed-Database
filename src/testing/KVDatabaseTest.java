package testing;

import database.KVdatabase;
import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;


public class KVDatabaseTest extends TestCase{

    KVdatabase db = new KVdatabase();
    public void setUp() {
        BasicConfigurator.configure();
        db = new KVdatabase();
    }

    public void tearDown() {
        db.clearStorage();
    }

    public void testInsert() {
        String key = "foo";
        String value = "bar";
        boolean expected = true;
        boolean actual = db.insertPair(key, value);
        assertEquals(expected, actual);
    }


    public void testGet() {
        db.insertPair("foo", "bar");
        String key = "foo";
        String expected = "bar";
        String actual = db.getValue(key);
        assertEquals(expected, actual);
    }

    public void testDelete() {
        db.insertPair("foo", "bar");
        String key = "foo";
        boolean expected = true;
        boolean actual = db.deletePair(key);
        assertEquals(expected, actual);
    }

    public void testUpdate() {
        String key = "foo";
        db.insertPair(key, "bar");
        db.insertPair(key, "yeah");
        String expected = "yeah";
        String actual = db.getValue(key);
        assertEquals(expected, actual);
    }

    public void testClear() {
        String key = "foo";
        String key2 = "foo2";
        db.insertPair(key, "bar");
        db.insertPair(key2, "bar2");

        boolean actual = db.clearStorage();
        boolean expected = true;
        assertEquals(expected, actual);

    }
}
