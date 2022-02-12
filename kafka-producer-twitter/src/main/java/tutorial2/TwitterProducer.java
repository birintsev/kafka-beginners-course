package tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public static final List<String> TERMS = Lists.newArrayList("bitcoin", "kafka", "java");

    private static final Logger LOGGER = LoggerFactory.getLogger(
        TwitterProducer.class
    );

    private final String consumerKey = "I do not have a tweeter dev account";

    private final String consumerSecret = "I do not have a tweeter dev account";

    private final String token = "I do not have a tweeter dev account";

    private final String secret = "I do not have a tweeter dev account";

    public TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        LOGGER.info("setup");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // create a twitter client
        Client client = new NoTwitterAccountClient(createTwitterClient(msgQueue), msgQueue);
        client.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Stopping application");

            LOGGER.info("Shutting down client from twitter...");
            client.stop();
            LOGGER.info("Closing producer...");
            producer.close();
            LOGGER.info("Done...");
        }));

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                LOGGER.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        LOGGER.error("Something bad happened", e);
                    }
                });
            }
        }
        LOGGER.info("End of application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "localhost:9092";

        // create Producer properties
        // https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // "bootstrap.servers"
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // "key.serializer"
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // "value.serializer"

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(TERMS);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
            .name("Hosebird-Client-01") // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }
}
