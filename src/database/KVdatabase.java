package database;

import app_kvServer.KVServer;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class for manipulating key-value store database using a simple key to file mapping
 * for each pair.
 */
public class KVdatabase implements IKVDatabase{

    /**
     * File channels are thread safe, allowing concurrent reads and locked writes
     * Store the file channel for all open keys in the system
     */
    ConcurrentHashMap<String, FileChannel> channels;
    KVServer sv;
    public String keyPath;
    String defaultPath = "./src/KVStorage";
    public static Logger logger = Logger.getLogger(KVdatabase.class);

    /**
     * Constructor with default path
     * @param sv
     */
    public KVdatabase(KVServer sv) {
        this(sv, null);
    }

    /**
     * Constructor with user-defined path
     * @param sv
     * @param dir
     */
    public KVdatabase(KVServer sv, String dir) {
        this.sv = sv;

        //Initialize datapath to default or user-specified
        if (dir == "" || dir == null)
            this.keyPath = defaultPath;
        else
            this.keyPath = dir;

        logger.debug("Initializing database with path: " + this.keyPath);

        //create datapath directory if it doesn't exist
        if (!Files.isDirectory(Paths.get(this.keyPath))){

            try {
                Files.createDirectory(Paths.get(this.keyPath));
            }
            catch(Exception e){
                logger.warn("Error while initializing database: ", e);
            }
        }
        channels = new ConcurrentHashMap<>();
    }

    /**
     * Constructor with no server initialization
     */
    public KVdatabase() {
        this(null, null);
    }

    @Override
    public String getValue(String key, boolean withSub) {
        String kvFile =  keyPath + "/" +  key + ".txt";
        String value = "";
        Path path = Paths.get(kvFile);
        FileChannel reader = channels.get(key);
        try {
            //open the file channel and read
            if (reader == null) {
                reader = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                channels.put(path.toString(), reader);

            }
            reader.position(0);

            ByteArrayOutputStream out = new ByteArrayOutputStream();

            int bufferSize = 1024;
            ByteBuffer buff = ByteBuffer.allocate(bufferSize);

            while (reader.read(buff) > 0) {
                out.write(buff.array(), 0, buff.position());
                buff.clear();
            }
            if (!withSub) {
                value = new String(out.toByteArray(), StandardCharsets.UTF_8);
                int idx = value.indexOf("\n");
                value = value.substring(idx + 1);
            }


        }
        catch(Exception e) {
            if (!(e instanceof NoSuchFileException))
                logger.warn("Exception thrown when trying to read key-value pair: ", e);
            return null;
        }

        return value;

    }

    @Override
    public boolean insertPair(String key, String value, boolean withSub) throws Exception{
        String kvFile = keyPath + "/" +  key + ".txt";
        boolean exists = true;
        Path path = Paths.get(kvFile);

        try {
            FileChannel writer = channels.get(path.toString());
            List<String> subs = null;
            String subscribers = "\n";
            if (writer == null) {
                exists = false;
                writer = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                channels.put(path.toString(), writer);
            }
            else {
                subs = getSubscribers(key);
            }

            if (subs != null){
                subscribers = subs.toString();

                subscribers = subscribers.replaceAll("\\s", "");
                subscribers= subscribers.replace("[", "");
                subscribers = subscribers.replace("]", "\n");
            }

            if (!withSub)
                value = new StringBuilder(value).insert(0, subscribers).toString();


            writer.position(0);
            writer.truncate(0);
            writer.write(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
        }
        catch (Exception e) {
//            if (sv != null)
            logger.warn("Exception thrown when writing to the key-value store:", e);
            throw new Exception("Write Exception");
        }
        return exists;
    }

    @Override
    public boolean deletePair(String key) throws IOException {
        String kvFile = keyPath + "/" +  key + ".txt";
        Path path = Paths.get(kvFile);
        boolean success = false;
        try {
            FileChannel channel = channels.get(key);
            if (channel != null) channel.close();
            channels.remove(kvFile);
            Files.delete(path);
            success = true;


        }
        catch(Exception e){
            if (e instanceof NoSuchFileException) {
                logger.debug("The file you are attempting to delete does not exist");
                throw e;
            }
            logger.warn("An Exception occured during deletion: ", e);
            return false;
        }

        if (!success){
            logger.info("The file you are attempting to delete does not exist");
        }

        return success;

    }
    @Override
    public boolean clearStorage() {
        return clearStorage(true);
    }

    public boolean clearStorage(boolean deleteDir) {
        Path rootPath = Paths.get(keyPath);


        channels.clear();
        try{
            Stream<Path> pathStream = Files.walk(rootPath);
            List<Path> pathList = pathStream.collect(Collectors.<Path>toList());
            for (Path curr:pathList){
                if (Files.isDirectory(curr)) continue;
                String key  = curr.getFileName().toString();
                String keySub = key.substring(0, 4);
                if (keySub.equals(".nfs")) continue;
                FileChannel channel = channels.get(key);

                if (channel != null) {
                    channel.close();
                    channels.remove(key);
                }
                boolean success = Files.deleteIfExists(curr);
                if (!success) {
                    return false;
                }
            }
            Path defPath = Paths.get(defaultPath);
            if (!rootPath.toString().equals(defPath.toString()))
                if (deleteDir) Files.delete(rootPath);

        }
        catch (Exception e){
            logger.warn("Exception occurred when deleting files: ", e);
            return false;
        }
        return true;
    }

    public String[] getAllKeys() {
        Path rootPath = Paths.get(keyPath);
        List<String> res = new ArrayList<>();
        try {
            Stream<Path> pathStream = Files.walk(rootPath);
            List<Path> pathList = pathStream.collect(Collectors.<Path>toList());
            for (Path curr : pathList) {
                if (Files.isDirectory(curr)) continue;

                String key = curr.getFileName().toString();
                String keySub = key.substring(0, 4);
                if (keySub.equals(".nfs")) continue;
                res.add(key.replace(".txt", ""));

            }

        } catch (Exception e) {
            logger.warn("Exception occurred when deleting files: ", e);
        }
        return res.toArray(new String[0]);
    }

    public List<String> getSubscribers(String key){
        String kvFile =  keyPath + "/" +  key + ".txt";
        List<String> subs;
        Path path = Paths.get(kvFile);
        FileChannel reader = channels.get(path.toString());
        if (reader == null){
            return null;
        }
        else {
            try {
                reader.position(0);

                ByteArrayOutputStream out = new ByteArrayOutputStream();

                int bufferSize = 1024;
                ByteBuffer buff = ByteBuffer.allocate(bufferSize);

                while (reader.read(buff) > 0) {
                    out.write(buff.array(), 0, buff.position());
                    buff.clear();
                }
                String value = new String(out.toByteArray(), StandardCharsets.UTF_8);
                int idx = value.indexOf("\n");
                if (idx == 0) return null;
                value = value.substring(0, idx);
                subs = Arrays.asList(value.split(","));
                subs = new ArrayList<>(subs);

                return subs;
            }
            catch(IOException ioe){
                logger.warn("Exception thrown when trying to read key-value pair: ", ioe);
                return null;
            }
        }
    }
    public void addSubscriber(String key, String clientID) throws Exception {
        List<String> subs = getSubscribers(key);
        if (subs == null || !subs.contains(clientID)){
            if (subs == null){
                subs = new ArrayList<>();
                subs.add(clientID);
            }
            else {
                subs.add(clientID);
            }
            String list = subs.toString();
            list = list.replaceAll("\\s", "");
            list = list.replace("[", "");
            list = list.replace("]", "\n");

            String kvFile = keyPath + "/" +  key + ".txt";
            Path path = Paths.get(kvFile);
            StringBuilder value = new StringBuilder(new String(getValue(key, false)));
            value.insert(0, list);

            FileChannel channel = channels.get(path.toString());
            channel.position(0);
            channel.truncate(0);
            channel.write(ByteBuffer.wrap(value.toString().getBytes(StandardCharsets.UTF_8)));
        }
    }

    public boolean removeSubscriber(String key, String clientID) throws Exception {
        List<String> subs = getSubscribers(key);
        if (subs.contains(clientID)){
            subs.remove(clientID);
            String list = subs.toString();
            list = list.replaceAll("\\s", "");
            list = list.replace("[", "");
            list = list.replace("]", "\n");

            StringBuilder value = new StringBuilder(new String(getValue(key, false)));
            value.insert(0, list);

            String kvFile = keyPath + "/" +  key + ".txt";
            Path path = Paths.get(kvFile);
            FileChannel channel = channels.get(path.toString());
            channel.position(0);
            channel.truncate(0);
            channel.write(ByteBuffer.wrap(value.toString().getBytes(StandardCharsets.UTF_8)));
            return true;
        }
        return false;
    }

}


