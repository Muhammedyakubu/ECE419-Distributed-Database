package app_kvServer;

/**
 * Class responsible for managing individual key ranges of a server
 */
public class Range {
    public int start;
    public int end;

    /**
     * Constructors initialize to a definite range or to -1 indicating unincorporated server
     * @param first
     * @param second
     */
    public Range(int first, int second){
        start = first;
        end = second;
    }
    public Range() {
        start = -1;
        end = -1;
    }
    /**
     * Updates the key range of a server
     * @param first
     * @param second
     */
    public void updateRange(int first, int second){
        start = first;
        end = second;
    };

    /**
     * Tests if a hash value is in the key range of a server
     * @param hash
     * @return
     */
    public boolean inRange(int hash){
        if (hash <= end && hash > start) return true;

        else return false;

    }


}
