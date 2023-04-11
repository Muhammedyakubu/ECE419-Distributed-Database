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

    public void tearDown() {
        db.clearStorage();
    }

}
