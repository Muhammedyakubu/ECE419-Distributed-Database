package shared.comms;

import shared.messages.KVMessage;

import java.io.IOException;
import java.net.Socket;

/**
 * This class implements methods to send and receive KVMessages over a TCP socket
 */
public interface ICommModule {

    /**
     * Method sends a KVMessage using this socket.
     * @param msg the message that is to be sent.
     * @param socket the socket over which the message should be sent
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(KVMessage msg, Socket socket) throws IOException;

    /**
     * Method receives a KVMessage using this socket.
     * @param socket the socket over which the message should be sent
     * @return the received message as a KVMessage object
     * @throws IOException some I/O error regarding the input stream
     */
    public KVMessage receiveMessage(Socket socket) throws IOException;

    /**
     * Closes the given socket
     * @param socket the socket to close
     */
    public void closeSocket(Socket socket) throws IOException;
}
