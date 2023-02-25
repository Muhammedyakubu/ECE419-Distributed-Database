package shared.messages;

import app_kvServer.Range;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
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

    public void addServer(String serverAddPort, int startpoint, int endpoint){

    }

    public void addServer(String server, String port, int startpoint, int endpoint){

    }

    public String toString() {
        String returned_string = "";
        for (int i = 0; i < this.metadata.size(); i++)
        {
            returned_string = metadata.get(i).p1 + " " + metadata.get(i).p2.start + " " + metadata.get(i).p2.end + ",";
        }

        return returned_string;
    }

    public KVMetadata toMetadata(String value){
        KVMetadata metadata = new KVMetadata();

        String[] tokens = value.split(",");

        for(int i = 0; i < tokens.length; i++)
        {
            String[] vals = tokens[i].split(" ");
            String serverAddPort = vals[0];
            Range range = new Range(Integer.parseInt(vals[1]), Integer.parseInt(vals[2]));
            Pair<String, Range> new_pair = new Pair<String, Range>();
            new_pair.setValue(serverAddPort, range);
            metadata.metadata.add(new_pair);
        }

        return metadata;
    }
}
