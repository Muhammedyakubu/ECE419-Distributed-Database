package database;

import app_kvServer.KVServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;

/**
 * Class for manipulating key-value store database using a simple key to file mapping
 * for each pair.
 * TODO Test read/write locks
 */
public class KVdatabase implements IKVDatabase{

    KVServer sv;
    String keyPath;
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
    }

    public KVdatabase() {
        this(null, null);
    }

    @Override
    public String getValue(String key) {
        String kvFile =  keyPath + "/" +  key + ".txt";
        Path path = Paths.get(kvFile);
        String value;

        try {
            byte[] bytes = Files.readAllBytes(path);
            value = new String(bytes);
        }
        catch (IOException e){
            sv.logger.warn("Exception thrown when reading key-value pair: ", e);
            return null;
        }
        return value;
    }

    @Override
    public boolean insertPair(String key, String value) throws Exception{
        String kvFile = keyPath + "/" +  key + ".txt";
        Path path = Paths.get(kvFile);
        boolean exists;
        try {
            exists = Files.exists(path);
            if (!exists) {
                Files.createFile(path);
            }
            byte[] bytes = value.getBytes();
            Files.write(path, bytes);
        }
        catch(Exception e){
            if (sv != null)
                sv.logger.warn("Exception thrown when writing to the key-value store:", e);
            throw new Exception("Write Exception");
        }

        return exists; //returns true if update and false if new insertion
    }

    @Override
    public boolean deletePair(String key) {
        String kvFile = keyPath + "/" +  key + ".txt";
        Path path = Paths.get(kvFile);
        boolean success;
        try {
            success = Files.deleteIfExists(path);
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


        try{
            Stream<Path> pathStream = Files.walk(rootPath);
            List<Path> pathList = pathStream.collect(Collectors.<Path>toList());
            //List<Path> pathList = (Files.walk(rootPath)).toList();
            for (Path curr:pathList){
                if (Files.isDirectory(curr)) continue;
                boolean success = Files.deleteIfExists(curr);
                if (!success) {
                    return false;
                }
            }

        }
        catch (Exception e){
            if (sv != null)
                sv.logger.warn("Exception occurred when deleting files: ", e);
            return false;
        }

        return true;
    }

}
