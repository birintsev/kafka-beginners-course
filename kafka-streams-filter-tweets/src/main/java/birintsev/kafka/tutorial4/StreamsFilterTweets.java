package birintsev.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class StreamsFilterTweets {

    private static final JsonParser JSON_PARSER = new JsonParser();

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamsFilterTweets.class);

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) -> {
            // filter tweets which has a user over 10000 followers
            boolean isTweetImportant = extractUserFollowersInTweet(jsonTweet) > 10_000;
            LOGGER.info(String.format("The tweet below is %s: %s", (isTweetImportant ? "IMPORTANT" : "NOT important"), jsonTweet));
            return isTweetImportant;
        });
        filteredStream.to("important_tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start our streams application
        kafkaStreams.start();
    }

    private static int extractUserFollowersInTweet(String tweetJson) {
        try {
            return JSON_PARSER
                .parse(tweetJson)
                .getAsJsonObject()
                .get("user")
                .getAsJsonObject()
                .get("followers_count")
                .getAsInt();
        } catch (Exception e) {
            LOGGER.error("Bad data came in json. Unable to get \"followers_count\" in the \"user\" object", e);
            return 0;
        }
    }
}
