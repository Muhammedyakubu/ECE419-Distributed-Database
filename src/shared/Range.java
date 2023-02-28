package shared;

import java.math.BigInteger;

/**
 * Class responsible for managing individual key ranges of a server
 */
public class Range {
    public BigInteger start;
    public BigInteger end;

    /**
     * Constructors initialize to a definite range or to -1 indicating unincorporated server
     * @param first
     * @param second
     */
    public Range(BigInteger first, BigInteger second){
        start = first;
        end = second;
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
        if (start.compareTo(end) > 1) {
            if (hash.compareTo(BigInteger.ZERO) >=0 && hash.compareTo(end) <= 0) return true;
            else if (hash.compareTo(start) == 1) return true;
            else return false;
        }

        if (hash.compareTo(end) <= 0  && hash.compareTo(start) >= 0) return true;
        else return false;

    }


}
