package database;

import app_kvServer.KVServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import java.util.stream.Collectors;

/**
 * Class for manipulating key-value store database using a simple key to file mapping
 * for each pair.
 * TODO Test read/write locks
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

    /**
     * Three constructors provided: with server and custom store location, with default store location,
     * and with no server
     */
    public KVdatabase(KVServer sv) {
        this(sv, null);
    }
    public KVdatabase(KVServer sv, String dir) {
        this.sv = sv;

        //Initialize datapath to default or user-specified
        if (dir == "" || dir == null)
            this.keyPath = defaultPath;
        else
            this.keyPath = dir;

        //create datapath directory if it doesnt exist
        if (!Files.isDirectory(Paths.get(this.keyPath))){
            try {
                Files.createDirectory(Paths.get(this.keyPath));
            }
            catch(Exception e){
                if (sv != null)
                    sv.logger.warn("Error while initializing database: ", e);
            }
        }
        channels = new ConcurrentHashMap<>();
    }

    public KVdatabase() {
        this(null, null);
    }

    @Override
    public String getValue(String key) {
        String kvFile =  keyPath + "/" +  key + ".txt";
        String value = "";
        Path path = Paths.get(kvFile);
        FileChannel reader = channels.get(key);
        try {
            if (reader == null) {
                reader = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
                channels.put(kvFile, reader);

            }

            /*boolean exists = Files.exists(path); //next check if file was persisted but not in hashmap
            if (exists) {
                reader = FileChannel.open(kvFile, "rw");
                channels.put(kvFile, reader);
            }
            else {
                if (sv != null)
                    sv.logger.warn("The file you are locking for does not exist");
                return null; //file does not exist
            }*/
            reader.position(0);

            ByteArrayOutputStream out = new ByteArrayOutputStream();

            int bufferSize = 1024;
            ByteBuffer buff = ByteBuffer.allocate(bufferSize);

            while (reader.read(buff) > 0) {
                out.write(buff.array(), 0, buff.position());
                buff.clear();
            }
            value = new String(out.toByteArray(), StandardCharsets.UTF_8);


        }
        catch(Exception e) {
            if (sv != null)
                sv.logger.warn("Exception thrown when trying to read key-value pair: ", e);
            return null;
        }

        return value;

    }

    @Override
    public boolean insertPair(String key, String value) throws Exception{
        String kvFile = keyPath + "/" +  key + ".txt";
        boolean exists = true;
        Path path = Paths.get(kvFile);

        try {
            FileChannel writer = channels.get(kvFile);
            if (writer == null) {
                exists = false;
                writer = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                channels.put(kvFile, writer);
            }

            writer.position(0);
            writer.truncate(0);
            writer.write(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));


            /*/perform write operation
            FileChannel channel = writer.getChannel();
            ByteBuffer buff = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
            channel.write(buff);
            channel.truncate(value.getBytes("UTF-8").length); //remove excess in case of update
            channel.position(0);//set position to zero*/
        }
        catch (Exception e) {
            if (sv != null)
                sv.logger.warn("Exception thrown when writing to the key-value store:", e);
            throw new Exception("Write Exception");
        }
        return exists;
    }

    @Override
    public boolean deletePair(String key) {
        String kvFile = keyPath + "/" +  key + ".txt";
        Path path = Paths.get(kvFile);
        boolean success;
        try {
            FileChannel channel = channels.get(key);
            if (channel != null) channel.close();
            channels.remove(kvFile);
            Files.delete(path);
            success = true;


        }
        catch(Exception e){
            if (sv != null)
                sv.logger.warn("An Exception occured during deletion: ", e);
            return false;
        }

        if (!success){
            if (sv != null)
                sv.logger.info("The file you are attempting to delete does not exist");
        }

        return success;

    }

    @Override
    public boolean clearStorage() {
        Path rootPath = Paths.get(keyPath);


        channels.clear();
        try{
            Stream<Path> pathStream = Files.walk(rootPath);
            List<Path> pathList = pathStream.collect(Collectors.<Path>toList());
            //List<Path> pathList = (Files.walk(rootPath)).toList();
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

        }
        catch (Exception e){
            if (sv != null)
                sv.logger.warn("Exception occurred when deleting files: ", e);
            else
                e.printStackTrace();
            return false;
        }


        return true;
    }

}
