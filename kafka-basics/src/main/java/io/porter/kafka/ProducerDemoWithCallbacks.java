package io.porter.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        System.out.println("I am a kafka producer");

        log.info("testing logger");

        Properties property = new Properties();

        property.setProperty("bootstrap.servers", "localhost:19092");
        property.setProperty("key.serializer", StringSerializer.class.getName());
        property.setProperty("value.serializer", StringSerializer.class.getName());
        property.setProperty("batch.size", "400");


        KafkaProducer<String, String> producer = new KafkaProducer<>(property);

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_topic", "hello ELIS " + i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {

                        if (e == null) {
                            log.info("Received new metadata \n" +
                                    "topic : " + metadata.topic() + "\n" +
                                    "offset : " + metadata.offset() + "\n" +
                                    "partition : " + metadata.partition() + "\n" +
                                    "timestamp : " + metadata.timestamp()

                            );
                        } else {
                            log.info("Error while producing " + e);
                        }


                    }
                });


            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        producer.flush();

        producer.close();
    }
}
