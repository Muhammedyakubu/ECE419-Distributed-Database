package shared.messages;

import shared.Range;

import java.math.BigInteger;
import java.util.List;
import java.util.Vector;

import static shared.MD5.getHash;

public class KVMetadata {

    //LIST ORDERED BY SERVER/PORT HASHES
    //WHEN INSERTING, SEARCH FOR SERVER WITH KEY RANGE AND UPDATE THOSE TWO RANGES

    List<Pair<String, Range>> metadata = new Vector<Pair<String, Range>>(1);

    public KVMetadata(){

    }
    public KVMetadata(int port, String address, Range range){
        String key = address + ":" + String.valueOf(port);
        Pair<String, Range> new_pair = new Pair<String, Range>();
        new_pair.setValue(key, range);
        metadata.add(new_pair);
    }

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

    public void addServer(String server, String port){
        String serverAddPort = server + ":" + port;
        BigInteger hash = getHash(serverAddPort);
        //DON'T WANT TO IMPLEMENT THIS YET UNTIL WE'RE IMPLEMENTING REBALANCING
        //PROBABLY WANT TO PASS SOME THINGS BY REFERENCE SO WE CAN RETURN OTHER THINGS FROM THIS METHOD
    }

    /**
     * Finds server associated with given key
     * @param key
     * @return <ip>:<port> String
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
