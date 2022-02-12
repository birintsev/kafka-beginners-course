package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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
            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;

            // create a producer record
            ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, key, value);

            // send data
            producer.send(
                record,
                (recordMetadata, e) -> { // callback
                    // executes every time a record is successfully sent or an exception thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Key " + key);
                        logger.info(
                            "Received new metadata: " + System.lineSeparator() +
                                "Topic = " + recordMetadata.topic() + System.lineSeparator() +
                                "Partition = " + recordMetadata.partition() + System.lineSeparator() +
                                "Offset = " + recordMetadata.offset() + System.lineSeparator() +
                                "Timestamp = " + recordMetadata.timestamp()
                        );
                        /*logger.info(
                            "Received new metadata: " + "Key = " + key + " Partition = " + recordMetadata.partition()
                        );*/
                        /*
                            Received new metadata: Key = id_0 Partition = 1
                            Received new metadata: Key = id_1 Partition = 0
                            Received new metadata: Key = id_2 Partition = 2
                            Received new metadata: Key = id_3 Partition = 0
                            Received new metadata: Key = id_4 Partition = 2
                            Received new metadata: Key = id_5 Partition = 2
                            Received new metadata: Key = id_6 Partition = 0
                            Received new metadata: Key = id_7 Partition = 2
                            Received new metadata: Key = id_8 Partition = 1
                            Received new metadata: Key = id_9 Partition = 2
                        */
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            ).get(); // block the .send() to make it synchronous - do not do in production!
        }

        producer.flush();
        producer.close();
    }
}
