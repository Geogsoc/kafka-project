package io.porter.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiMediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";
        Properties property = new Properties();

        property.setProperty("bootstrap.servers", bootstrapServers);
        property.setProperty("key.serializer", StringSerializer.class.getName());
        property.setProperty("value.serializer", StringSerializer.class.getName());

        //high throughput config
        property.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        //1024 ==1KB
        property.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));
        property.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");



        KafkaProducer<String, String> producer = new KafkaProducer<>(property);

        String topic = "wikimedia.recentchange";

        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create("https://stream.wikimedia.org/v2/stream/recentchange"));

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder);

        BackgroundEventSource eventSource = builder.build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(10);


    }
}
