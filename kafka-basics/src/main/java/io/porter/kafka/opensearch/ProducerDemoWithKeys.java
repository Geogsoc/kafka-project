package io.porter.kafka.opensearch;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        System.out.println("I am a kafka producer");

        log.info("testing logger");

        Properties property = new Properties();

        property.setProperty("bootstrap.servers", "localhost:19092");
        property.setProperty("key.serializer", StringSerializer.class.getName());
        property.setProperty("value.serializer", StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<>(property);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {

                String topic = "demo_topic";
                String key = "id_" + i;
                String value = "hello ELIS " + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {

                        if (e == null) {
                            log.info("key: " + key + "| partition : " + metadata.partition()


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
