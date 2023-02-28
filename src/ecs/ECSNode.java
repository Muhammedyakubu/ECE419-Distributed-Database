package ecs;

import shared.Range;
import shared.comms.CommModule;
import shared.messages.KVMessage;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;

public class ECSNode implements IECSNode, Runnable{
    private InetAddress address;
    private String hostAddress;
    private int port;
    private Range hashRange;
    private Socket socket;
    private boolean running;

    public ECSNode(Socket socket, String hostAddress, int port, Range hashRange) throws IOException {
        this.socket = socket;
        this.hostAddress = hostAddress;
        this.address = InetAddress.getByName(hostAddress);
        this.port = port;
        this.hashRange = hashRange;
        this.running = false;
    }

    public void sendMessage(KVMessage message) throws IOException {
        CommModule.sendMessage(message, this.socket);
    }

    public KVMessage receiveMessage() throws IOException {
        return CommModule.receiveMessage(this.socket);
    }

    /**
     * @return
     */
    @Override
    public String getNodeName() {
        return this.address.getHostAddress() + ":" + this.port;
    }

    /**
     * @return
     */
    @Override
    public String getNodeHost() {
        return this.hostAddress;
    }

    /**
     * @return
     */
    @Override
    public int getNodePort() {
        return this.port;
    }

    /**
     * @return
     */
    @Override
    public Range getNodeHashRange() {
        return this.hashRange;
    }

    /**
     *
     */
    @Override
    public void run() {

    }
}
