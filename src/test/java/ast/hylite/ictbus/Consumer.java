package ast.hylite.ictbus;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

    public static void main(final String[] args) {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        final KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        final List<String> topics = new ArrayList<String>();
        // Add topics to subscribe
        topics.add("test-topic");
        kafkaConsumer.subscribe(topics);
        try {
            while (true) {
                final ConsumerRecords<?, ?> records = kafkaConsumer.poll(10);
                for (final Object record : records) {
                    final ConsumerRecord<?, ?> r = (ConsumerRecord<?, ?>) record;
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", r.topic(), r.partition(),
                            r.value()));
                }
            }
        } catch (final Exception e) {
            System.out.println(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }
}
