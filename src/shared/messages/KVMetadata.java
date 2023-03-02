package shared.messages;

import shared.Range;

import java.math.BigInteger;
import java.util.List;
import java.util.Vector;

import static shared.MD5.getHash;

//IMPLEMENT REMOVE NODE THAT RETURNS SUCCESSOR
//IMPLEMENT GET RANGE FROM SERVER
public class KVMetadata implements IKVMetadata{

    //LIST ORDERED BY SERVER/PORT HASHES
    //WHEN INSERTING, SEARCH FOR SERVER WITH KEY RANGE AND UPDATE THOSE TWO RANGES

    List<Pair<String, Range>> metadata = new Vector<Pair<String, Range>>(1);

    /**
     * Default constructor
     */
    public KVMetadata(){

    }

    /**
     * Constructor with initial (single) metadata value
     */
    public KVMetadata(int port, String address, Range range){
        String key = address + ":" + String.valueOf(port);
        Pair<String, Range> new_pair = new Pair<String, Range>();
        new_pair.setValue(key, range);
        metadata.add(new_pair);
    }

    /**
     * Constructor with string for use of String to Metadata conversion
     */
    public KVMetadata(String value){

        String[] tokens = value.split(";");

        for(int i = 0; i < tokens.length; i++)
        {
            String[] vals = tokens[i].split(",");
            BigInteger start = new BigInteger(vals[0], 16);
            BigInteger end = new BigInteger(vals[1], 16);
            this.addServer(vals[2], start, end);
        }
    }

    public void addServer(String serverAddPort, BigInteger startpoint, BigInteger endpoint){
        Range range = new Range(startpoint, endpoint);
        Pair<String, Range> new_pair = new Pair<String, Range>();
        new_pair.setValue(serverAddPort, range);
        metadata.add(new_pair);
    }

    /**
     * adds a new server to the metadata and returns the new server's
     * keyrange and its successor's address info
     * This function will also have to deal with when the new server is the
     * first server, in which case it will return null for the successor's
     * address info
     *
     * @param serverAddress the address of the server
     * @param port          the port the server is listening on
     * @return a pair containing the new server's keyrange and its successor
     */
    public Pair<Range, String> addServer(String serverAddress, int port){

        String serverAddPort = serverAddress + ":" + port;
        BigInteger hash = getHash(serverAddPort);
        BigInteger start = hash.add(BigInteger.ONE);
        Range range;
        Pair rangeServer = new Pair();
        Pair newEntry = new Pair();

        //If this is the first server being added...
        if(metadata.isEmpty()){
            addServer(serverAddPort, start, hash);
            range = new Range(start, hash);
            rangeServer.setValue(range, null);
            return rangeServer;
        }

        else{
            for (int i = 0; i < metadata.size(); i++)
            {
                if(metadata.get(i).p2.inRange(hash)) {
                    range = new Range(metadata.get(i).p2.start, hash);
                    metadata.get(i).p2.updateStart(start);
                    newEntry.setValue(serverAddPort, range);
                    metadata.add(i, newEntry);
                    rangeServer.setValue(range, metadata.get(i).p1);
                    return rangeServer;
                }
            }
        }
        //Shouldn't get here
        return null;
    }

    /**
     * removes a server from the metadata and returns the successor's
     * keyrange and address info
     *
     * @param serverAddress the address of the server
     * @param port          the port the server is listening on
     * @return a pair containing the removed server's successor's <addr>:<port> and the successor's keyrange
     */

    public String findServer(String key){
        BigInteger hash = getHash(key);
        for (int i = 0; i < metadata.size(); i++)
        {
            if(metadata.get(i).p2.inRange(hash))
                return metadata.get(i).p1;
        }
        return null;
    }

    public String toString() {
        String returned_string = "";
        for (int i = 0; i < metadata.size(); i++)
        {
            returned_string = returned_string + metadata.get(i).p2.start.toString(16) + "," + metadata.get(i).p2.end.toString(16) + "," + metadata.get(i).p1 + ";";
        }

        return returned_string;
    }

}
