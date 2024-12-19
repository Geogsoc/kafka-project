package io.porter.kafka.opensearch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a kafka consumer");

        String groupId = "my-java-application";

        String topic = "demo_topic";

        Properties property = new Properties();

        property.setProperty("bootstrap.servers", "localhost:19092");
        property.setProperty("key.deserializer", StringDeserializer.class.getName());
        property.setProperty("value.deserializer", StringDeserializer.class.getName());

        property.setProperty("group.id", groupId);
        property.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(property);

        consumer.subscribe(Arrays.asList(topic));

        while (true) {

            log.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {

                log.info("Key: " + record.key() + " | Value: " + record.value());
                log.info("Partition: " + record.partition() + " | Offset: " + record.offset());

            }


        }

    }
}
