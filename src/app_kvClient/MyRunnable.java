package app_kvClient;

public class MyRunnable implements Runnable {
    public KVClient client;

        public MyRunnable(KVClient client) {
            this.client = client;
        }

    @Override
    public void run() {
//        printError("test");
        client.run_listener();
    }
}
