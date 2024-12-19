package io.porter.kafka.opensearch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("I am a kafka producer");

        log.info("testing logger");

        Properties property = new Properties();

        property.setProperty("bootstrap.servers", "localhost:19092");
        property.setProperty("key.serializer", StringSerializer.class.getName());
        property.setProperty("value.serializer", StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<>(property);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_topic", "hello ELIS");

        producer.send(producerRecord);

        producer.flush();

        producer.close();
    }
}
