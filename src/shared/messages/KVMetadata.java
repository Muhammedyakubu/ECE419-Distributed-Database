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

    public List<Pair<String, Range>> metadata = new Vector<Pair<String, Range>>(1);

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

    public Pair<String, Range> addServer(String serverAddress, int port){

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
            rangeServer.setValue(null, range);
            return rangeServer;
        }

        else{
            for (int i = 0; i < metadata.size(); i++)
            {
                if(metadata.get(i).p2.inRange(hash)) {
                    //System.out.println("test");
//                    Range testrange = new Range(BigInteger.ZERO, BigInteger.ONE);
//                    rangeServer.setValue("hi", testrange);
//                    return rangeServer;
                    range = new Range(metadata.get(i).p2.start, hash);
                    metadata.get(i).p2.updateStart(start);
                    newEntry.setValue(serverAddPort, range);
                    metadata.add(i, newEntry);
                    rangeServer.setValue(metadata.get(i+1).p1, range);
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

    public Pair<String, Range> removeServer(String serverAddress, int port){

        String serverAddPort = serverAddress + ":" + port;
        BigInteger hash = getHash(serverAddPort);
        Pair rangeServer = new Pair();

        for (int i = 0; i < metadata.size(); i++)
        {
            if (metadata.size() == 1){
                metadata.remove(0);
                return null;
            }
            //if last elements, wraps
            if(i == metadata.size()-1){
                metadata.get(0).p2.updateStart(metadata.get(i).p2.start);
                rangeServer = metadata.get(0);
                metadata.remove(i);
                return rangeServer;
            }

            metadata.get(i+1).p2.updateStart(metadata.get(i).p2.start);
            rangeServer = metadata.get(i+1);
            metadata.remove(i);
            return rangeServer;
        }
        return null;
    }

    public String findServer(String key){
        BigInteger hash = getHash(key);
        for (int i = 0; i < metadata.size(); i++)
        {
            if(metadata.get(i).p2.inRange(hash))
                return metadata.get(i).p1;
        }
        return null;
    }

    public Range getRange(String serverAddPort){
        for (int i = 0; i < metadata.size(); i++)
        {
            if(metadata.get(i).p1.compareTo(serverAddPort) == 0)
                return metadata.get(i).p2;
        }
        return null;
    }

    public boolean isEmpty(){
        return metadata.isEmpty();
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
