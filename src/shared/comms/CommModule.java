package shared.comms;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public final class CommModule /*implements ICommModule*/ {

    // Is inheriting loggers okay?
    public static Logger logger = Logger.getRootLogger();
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private CommModule() {

    }

//    @Override
    public static void sendMessage(KVMessage msg, Socket socket) throws IOException {
        OutputStream output = socket.getOutputStream();

        byte[] msgBytes = msg.toByteArray();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
                + msg +"'");
    }

//    @Override
    public static KVMessage receiveMessage(Socket socket) throws IOException {
        InputStream input = socket.getInputStream();

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 13 && read != -1 && reading) {/* CR, disconnect, error */
            /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        /* Check for empty message indicating a disconnect */
        if(msgBytes.length < 2) {
            throw new IOException("Error! Connection lost!");
        }

        /* build final String */
        KVMessage msg = new KVMessage(msgBytes);
        logger.info("RECEIVE \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
                + msg + "'");
        return msg;
    }

//    @Override
    public static void closeSocket(Socket socket) throws IOException {
        if (socket == null) {
            logger.warn("cannot close null socket");
            return;
        }

        logger.info("closing the connection to "
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
                + " ...");
        socket.close();
        logger.info("connection closed!");
    }
}
