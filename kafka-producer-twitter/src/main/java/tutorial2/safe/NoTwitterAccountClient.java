package tutorial2.safe;

import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.StatsReporter;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

public class NoTwitterAccountClient implements Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        NoTwitterAccountClient.class
    );

    private final Client client;

    private final BlockingQueue<String> msgQueue;

    private boolean isConnected;

    public NoTwitterAccountClient(Client client, BlockingQueue<String> msgQueue) {
        this.client = client;
        this.msgQueue = msgQueue;

        new DummyTweetsGenerator(msgQueue).start();
    }

    @Override
    public void connect() {
        // throw new UnsupportedOperationException();

        try {
            threadSleepRandom(3500); // simulating connection delay
        } catch (InterruptedException e) {
            LOGGER.error("Unexpected error", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        isConnected = true;
        LOGGER.info("Connected");
    }

    @Override
    public void reconnect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
        // throw new UnsupportedOperationException();
        try {
            threadSleepRandom(500); // simulating connection delay
        } catch (InterruptedException e) {
            LOGGER.error("Unexpected error", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        isConnected = false;
        LOGGER.info("Stopped");
    }

    @Override
    public void stop(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamingEndpoint getEndpoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatsReporter.StatsTracker getStatsTracker() {
        throw new UnsupportedOperationException();
    }

    private static void threadSleepRandom(int maxMillis)
        throws InterruptedException {
        Thread.sleep(
            Math.abs(new Random(System.nanoTime()).nextInt(maxMillis))
        );
    }

    private static class DummyTweetsGenerator extends Thread {

        private final BlockingQueue<String> msgQueue;

        public DummyTweetsGenerator(BlockingQueue<String> msgQueue) {
            this.msgQueue = msgQueue;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String msg = String.format(
                        "{title: 'dummy tweet', subject: '%s', id: '%s'}",
                            getRandomMessageSubject(),
                            generateRandomMessageId()
                    );
                    msgQueue.put(msg);
                    threadSleepRandom(10000);
                }
            } catch (InterruptedException e) {
                LOGGER.error("Unexpected error", e);
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private int generateRandomMessageId() {
            return Math.abs(
                (int) System.nanoTime() %new Random(System.nanoTime()).nextInt()
            );
        }

        private String getRandomMessageSubject() {
            return TwitterProducer.TERMS.get(
                Math.abs((int) System.nanoTime()) % TwitterProducer.TERMS.size()
            );
        }
    }
}
