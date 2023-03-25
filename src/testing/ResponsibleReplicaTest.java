package testing;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;
import org.junit.Test;
import shared.MD5;
import shared.Range;
import shared.messages.KVMetadata;
import shared.messages.Pair;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Random;

public class ResponsibleReplicaTest extends TestCase {
    private KVServer[] servers;
    private Range[] ranges;
    private int numServers = 5;
    private KVMetadata metadata = new KVMetadata();

    public void setUp(){
        BasicConfigurator.configure();
        servers = new KVServer[numServers];
        ranges = new Range[numServers];

        for (int i = 0; i < numServers; i++) {
            servers[i] = new KVServer(8000 + i, 0, "None", false);
            metadata.addServer(servers[i].bindAddress, servers[i].getPort());
        }
        for (int i = 0; i < numServers; i++){
            servers[i].kvMetadata = new KVMetadata(metadata.toString());
            servers[i].keyRange = metadata.getRange(servers[i].bindAddress + ":" + servers[i].getPort());
            ranges[i] = metadata.metadata.get(i).getSecond();
        }

    }
    synchronized public boolean isReplicaResponsible(KVServer server, BigInteger key){
        Pair<String, Range> predecessor = server.kvMetadata.getNthSuccessor(server.bindAddress + ":" + Integer.toString(server.getPort()), -1);
        Pair<String, Range> predecessorTwo = server.kvMetadata.getNthSuccessor(server.bindAddress + ":" + Integer.toString(server.getPort()), -2);
        return (predecessor.getSecond().inRange(key) || predecessorTwo.getSecond().inRange(key));
    }

    /**
     * Generates a random hashkey and makes sure replicas who are responsible return true
     */
    public void testReplicaResponsible() {
        //generate random hash key
        Random random = new Random();
        int len = ranges[0].FFFF.bitLength();
        BigInteger hash = new BigInteger(len, random);
        hash.mod(ranges[0].FFFF.add(BigInteger.ONE));
        int next = 1;
        int secondNext = 2;

        //find indices of range
        int i;
        for (i = 0; i<numServers; i++){
            if (ranges[i].inRange(hash)){
                break;
            }
            else {
                next = secondNext;
                if (secondNext == numServers - 1){
                    secondNext = 0;
                }
                else {
                    secondNext++;
                }
            }
        }
        int successor = -1;
        int secondSuccessor = -1;
        //find kvServers that are the two successors
        for (int j = 0; j < numServers; j++){
            if (servers[j].keyRange.equals(ranges[next])){
                successor = j;
            }
            if (servers[j].keyRange.equals(ranges[secondNext])){
                secondSuccessor = j;
            }
        }

        for (int j = 0; j <numServers; j++){
            boolean result = isReplicaResponsible(servers[j], hash);
            if (j == successor || j == secondSuccessor){
                assertEquals(result, true);
            }
            else
                assertEquals(result, false);
        }



    }


}
