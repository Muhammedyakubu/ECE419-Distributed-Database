package testing;

import database.KVdatabase;
import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SubDatabaseTest extends TestCase {
    KVdatabase db;
    String testKey;
    String testValue;
    String realValue;
    String subs;

    public void setUp() {
        BasicConfigurator.configure();
//        db = new KVdatabase(null, "./src/KVStorage/test");
//        db.clearStorage(false);
        db = new KVdatabase();
        db.clearStorage();
        testValue = "Hey there this is the value";
        realValue = "Hey there this is the value";
        testKey = "testKey";
        try {
            db.insertPair(testKey, testValue, false);
            db.addSubscriber(testKey, "1");
            db.addSubscriber(testKey, "2");
            db.addSubscriber(testKey, "3");
            subs = "1,2,3";
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    public void testGetSubscribers() {

        List<String> actual = db.getSubscribers(testKey);
        String a[] = new String[] { "1","2","3" };
        List<String> expected = Arrays.asList(a);
        assertEquals(expected, actual);
    }

    public void testAddSubscriber(){
        String addition = "4";
        try {
            db.addSubscriber(testKey, addition);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        String a[] = new String[] {"1","2","3","4"};
        List<String> expected = Arrays.asList(a);
        List<String> actual = db.getSubscribers(testKey);
        assertEquals(expected, actual);

    }

    public void testRemoveSubscriber() {
        String removal = "3";
        try {
            db.removeSubscriber(testKey, removal);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        String a[] = new String[] {"1","2"};
        List<String> expected = Arrays.asList(a);
        List<String> actual = db.getSubscribers(testKey);
        assertEquals(expected, actual);
    }

    public void testGetWithSubscribers() {
        String actual = db.getValue(testKey, true);
        String expected = subs + "\n" + realValue;  // won't work if ran as a suite
        assertEquals(expected, actual);
    }

    public void testGetWithoutSubscribers() {
        String actual = db.getValue(testKey, false);
        String expected = realValue;
        assertEquals(expected, actual);
    }

    public void testPutWithSubs() {
        String newValue = "This is a new value";
        try {
            db.insertPair(testKey, subs + "\n" + newValue, true);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        String actual = db.getValue(testKey, true);
        String expected = subs + "\n" + newValue;
        assertEquals(expected, actual);
    }

    public void testPutWithoutSubs() {
        String newValue = "This is a new value";
        try {
            db.insertPair(testKey, newValue, false);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        String actual = db.getValue(testKey, true);
        String expected = subs + "\n" + newValue;
        assertEquals(expected, actual);
    }

    public void tearDown() {
        db.clearStorage();
    }

}
