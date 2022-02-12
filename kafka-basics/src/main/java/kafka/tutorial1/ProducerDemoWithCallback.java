package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "localhost:9092";

        // create Producer properties
        // https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // "bootstrap.servers"
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // "key.serializer"
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // "value.serializer"

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String, String> record =
                new ProducerRecord<>("first_topic", "hello world " + i);

            // send data
            producer.send(
                record,
                (recordMetadata, e) -> { // callback
                    // executes every time a record is successfully sent or an exception thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info(
                            "Received new metadata: " + System.lineSeparator() +
                                "Topic = " + recordMetadata.topic() + System.lineSeparator() +
                                "Partition = " + recordMetadata.partition() + System.lineSeparator() +
                                "Offset = " + recordMetadata.offset() + System.lineSeparator() +
                                "Timestamp = " + recordMetadata.timestamp()
                        );
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            );
        }

        producer.flush();
        producer.close();
    }
}
