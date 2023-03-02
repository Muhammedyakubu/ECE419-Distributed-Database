package shared;

import java.math.BigInteger;

/**
 * Class responsible for managing individual key ranges of a server
 */
public class Range {
    public BigInteger start;
    public BigInteger end;
    public final BigInteger FFFF = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);

    /**
     * Constructors initialize to a definite range or to -1 indicating unincorporated server
     * @param first
     * @param second
     */
    public Range(BigInteger first, BigInteger second){
        start = first;
        end = second;
    }
    public Range(String range){
        String[] values = range.split(",");
        this.start = new BigInteger(values[0], 16);
        this.end = new BigInteger(values[1], 16);
//        this.start = new BigInteger(1, values[0].getBytes());
//        this.end = new BigInteger(1, values[1].getBytes());
    }
    public Range() {
        start = null;
        end = null;
    }
    /**
     * Updates the key range of a server
     * @param first
     * @param second
     */
    public void updateRange(BigInteger first, BigInteger second){
        start = first;
        end = second;
    };

    public void updateStart(BigInteger first){
        start = first;
    };

    public void updateEnd(BigInteger second){
        end = second;
    };

    /**
     * Tests if a hash value is in the key range of a server
     * @param hash
     * @return
     */
    public boolean inRange(BigInteger hash){

        //handle corner case when keyrange wraps around zero>=
        if (start.compareTo(end) == 1) {
            if (hash.compareTo(BigInteger.ZERO) >=0 && hash.compareTo(end) <= 0) return true;
            else if (hash.compareTo(start) >= 0 || hash.compareTo(FFFF) <= 0) return true;
            else return false;
        }

        if (hash.compareTo(end) <= 0  && hash.compareTo(start) >= 0) return true;
        else return false;

    }

    /**
     * Converts range to string in format start:end
     * @return String
     */
    public String toString(){
        String res = start.toString(16) + "," + end.toString(16);
        return res;
    }



}
