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
        boolean expected = false;
        boolean actual = false;
        try {
            actual = db.insertPair(key, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertEquals(expected, actual);
    }


    public void testGet() {
        try {
            db.insertPair("foo", "bar");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String key = "foo";
        String expected = "bar";
        String actual = db.getValue(key);
        assertEquals(expected, actual);
    }

    public void testDelete() {
        try {
            db.insertPair("foo", "bar");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String key = "foo";
        boolean expected = true;
        boolean actual = db.deletePair(key);
        assertEquals(expected, actual);
    }

    public void testUpdate() {
        String key = "foo";
        try {
            db.insertPair(key, "bar");
            db.insertPair(key, "yeah");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String expected = "yeah";
        String actual = db.getValue(key);
        assertEquals(expected, actual);
    }

    public void testClear() {
        String key = "foo";
        String key2 = "foo2";
        try {
            db.insertPair(key, "bar");
            db.insertPair(key2, "bar2");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        boolean actual = db.clearStorage();
        boolean expected = true;
        assertEquals(expected, actual);

    }
}
