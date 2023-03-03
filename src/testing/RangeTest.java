package testing;

import org.junit.Test;
import junit.framework.TestCase;
import shared.Range;

import java.math.BigInteger;

import static shared.MD5.getHash;

public class RangeTest extends TestCase {

    Range range = new Range();
    public final BigInteger FFFF = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
    @Test
    public void testInRangeSingleHash(){

        BigInteger end = getHash("localhost:5000");
        BigInteger start = end.add(BigInteger.ONE);
        range.updateRange(start, end);
        BigInteger test = getHash("test");
        assertTrue(range.inRange(test) && range.inRange(start) && range.inRange(end));
    }

    @Test
    public void testInRangeWrapAroundZero(){

        BigInteger end = BigInteger.ONE;
        BigInteger start = getHash("localhost:5000");
        range.updateRange(start, end);
        BigInteger test = start.add(BigInteger.ONE);
        assertTrue(range.inRange(test));
    }

    @Test
    public void testInRangeBeginLastZeroEnd(){

        BigInteger end = BigInteger.TEN;
        BigInteger start = getHash("localhost:5000");
        range.updateRange(start, end);
        boolean test1 = range.inRange(start);
        boolean test2 = range.inRange(end);
        boolean test3 = range.inRange(BigInteger.ZERO);
        boolean test4 = range.inRange(FFFF);
        assertTrue(test1 && test2 && test3 && test4);
    }

    @Test
    public void testInRangeSpecific(){

        BigInteger end = new BigInteger("98715326301568607101067940198791688124", 16);
        BigInteger start = new BigInteger("180293372416575166452018847750613510822", 16);
        BigInteger hash = new BigInteger("105490587539310211567724693302530339539", 16);
        range.updateRange(start, end);
        boolean test1 = range.inRange(hash);
        assertFalse(test1 /*&& test2 && test3 && test4*/);
    }
}
