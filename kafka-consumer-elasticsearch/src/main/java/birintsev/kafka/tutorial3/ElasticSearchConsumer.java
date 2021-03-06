package birintsev.kafka.tutorial3;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private static final JsonParser JSON_PARSER = new JsonParser();

    private static final Gson GSON = new Gson();

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        while (true) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(100));

            BulkRequest bulkRequest = new BulkRequest();

            int recordsCount = records.count();

            LOGGER.info("Received " + recordsCount + " records");
            for (ConsumerRecord<String, String> record : records) {
                // where we insert data into ElasticSearch

                // 2 strategies

                // kafka generic ID
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter specific id
                String id;
                try {
                    id = extractIdFromTweet(record.value());
                } catch (Exception e) { // for dummy generated tweets
                    LOGGER.error(
                        "An error occurred while parsing a tweet from the twitter_tweets topic.", e
                    );
                    id = "dummy_tweet_dummy_generated_id_" + record.value().hashCode();
                }

                IndexRequest indexRequest = new IndexRequest(
                    "twitter",
                    "tweets",
                    id // to make consumer idempotent
                ).source(toJson(record.value()), XContentType.JSON);
                bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
            }
            if (recordsCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                LOGGER.info("Committing offsets...");
                consumer.commitSync();
                LOGGER.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.error("Unexpected error occurred", e);
                }
            }
        }

        // close the client gracefully
        // client.close();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer config
        // https://kafka.apache.org/documentation/#consumerconfigs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // "earliest/latest/none"
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable autocommit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static RestHighLevelClient createClient() {
        String hostname = "kafka-course-9276042925.us-east-1.bonsaisearch.net";
        String username = "oox8ryo5k9";
        String password = "99o0me1oh7";

        // don't do if you run a local ElasticSearch
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder =
            RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(
                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                );

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    private static String extractIdFromTweet(String tweetJson) {
        return JSON_PARSER.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

    private static String toJson(String value) {
        return GSON.toJson(new JsonPojo(value));
    }

    private static class JsonPojo {

        public final String data;

        public JsonPojo(String data) {
            this.data = data;
        }
    }
}
