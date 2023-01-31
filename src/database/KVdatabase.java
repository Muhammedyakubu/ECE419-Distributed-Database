package database;

import app_kvServer.KVServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Class for manipulating key-value store database using a simple key to file mapping
 * for each pair.
 * TODO Add concurrency (locking)
 */
public class KVdatabase implements IKVDatabase{

    KVServer sv;
    String keyPath;
    String defaultPath = "./src/KVStorage/";

    /**
     * Three constructors provided: with server and custom store location, with default store location,
     * and with no server
     */
    public KVdatabase(KVServer sv) {
        this.sv = sv;
        this.keyPath = defaultPath;
    }
    public KVdatabase(KVServer sv, String dir) {
        this.sv = sv;
        this.keyPath = dir;
    }

    public KVdatabase() {
        this.sv = null;
        this.keyPath = defaultPath;
    }

    @Override
    public String getValue(String key) {
        String kvFile =  keyPath + key + ".txt";
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
    public boolean insertPair(String key, String value){
        String kvFile = keyPath + key + ".txt";
        Path path = Paths.get(kvFile);
        try {
            boolean exists = Files.exists(path);
            if (!exists) {
                Files.createFile(path);
            }
            byte[] bytes = value.getBytes();
            Files.write(path, bytes);
        }
        catch(Exception e){
            if (sv != null)
                sv.logger.warn("Exception thrown when writing to the key-value store:", e);
            return false;
        }

        return true;
    }

    @Override
    public boolean deletePair(String key) {
        String kvFile = keyPath + key + ".txt";
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
            List<Path> pathList = Files.walk(rootPath).toList();
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
