package shared.messages;

import shared.Range;

import java.math.BigInteger;
import java.util.List;
import java.util.Vector;

public class KVMetadata {

    //LIST ORDERED BY SERVER/PORT HASHES
    //WHEN INSERTING, SEARCH FOR SERVER WITH KEY RANGE AND UPDATE THOSE TWO RANGES

    List<Pair<String, Range>> metadata = new Vector<Pair<String, Range>>(1);

    KVMetadata(){

    }
    KVMetadata(int port, String address, Range range){
        String key = address + ":" + String.valueOf(port);
        Pair<String, Range> new_pair = new Pair<String, Range>();
        new_pair.setValue(key, range);
        metadata.add(new_pair);
    }

    public void addServer(String serverAddPort, BigInteger startpoint, BigInteger endpoint){
        Range range = new Range(startpoint, endpoint);
        Pair<String, Range> new_pair = new Pair<String, Range>();
        new_pair.setValue(serverAddPort, range);
        metadata.add(new_pair);
    }

    public void addServer(String server, String port, BigInteger startpoint, BigInteger endpoint){
        String serverAddPort = server + ":" + port;
        Range range = new Range(startpoint, endpoint);
        Pair<String, Range> new_pair = new Pair<String, Range>();
        new_pair.setValue(serverAddPort, range);
        metadata.add(new_pair);
    }

    public String toString() {
        String returned_string = "";
        for (int i = 0; i < this.metadata.size(); i++)
        {
            returned_string = metadata.get(i).p1 + " " + metadata.get(i).p2.start.toString(16)
                    + " " + metadata.get(i).p2.end.toString(16) + ",";
        }

        return returned_string;
    }

    public KVMetadata toMetadata(String value){
        KVMetadata metadata = new KVMetadata();

        String[] tokens = value.split(",");

        for(int i = 0; i < tokens.length; i++)
        {
            String[] vals = tokens[i].split(" ");
            BigInteger start = new BigInteger(vals[1], 16);
            BigInteger end = new BigInteger(vals[2], 16);
            metadata.addServer(vals[0], start, end);
        }

        return metadata;
    }
}
