package ast.hylite.ictbus;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.json.JSONException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

/**
 * Class for general data transfer using Apache Kafka Producer API, Consumer API
 * and Streams API
 *
 * @author kew
 */
public class Kafka_IO {

    // Initialization of Kafka producer, consumer and streams
    final Properties producerprops = new Properties();
    final Properties consumerprops = new Properties();
    final Properties streamprops = new Properties();
    final StreamsBuilder kstreams = new StreamsBuilder();
    // Declarations
    private String bootstrapservers;
    private String clientid;
    private ArrayList<String> msg_keys = new ArrayList<String>();
    private ArrayList<String> msg_values = new ArrayList<String>();

    /**
     * Sources:
     * https://www.programcreek.com/java-api-examples/index.php?api=kafka.server.KafkaServer
     * https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
     * https://www.devglan.com/apache-kafka/apache-kafka-java-example
     * http://cloudurable.com/blog/kafka-tutorial-kafka-producer/index.html
     * http://cloudurable.com/blog/kafka-tutorial-kafka-consumer/index.html
     * https://ordina-jworks.github.io/kafka/2018/10/23/kafka-stream-introduction.html
     * https://kafka.apache.org/10/documentation/streams/developer-guide/
     **/

    // Initialization
    Kafka_IO(String _bootstrapservers) {
        set_bootstrapservers(_bootstrapservers);
    }

    /**
     * Set bootstrapservers for Kafka producer, consumer and streams API (e.g.
     * "localhost:9092")
     *
     * @param _bootstrapservers
     */
    private void set_bootstrapservers(String _bootstrapservers) {
        bootstrapservers = _bootstrapservers;
    }

    /**
     * Set Kafka producer properties
     */
    private void set_producerproperties() {

        producerprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        producerprops.put(ProducerConfig.ACKS_CONFIG, "all");
        producerprops.put(ProducerConfig.RETRIES_CONFIG, 0);
        // producerprops.put(ProducerConfig.CLIENT_ID_CONFIG, clientid);
        producerprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    }

    /**
     * Set Kafka consumer properties
     */
    public void set_consumerproperties(String groupid, int num_records, Boolean autocomm) {

        consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        consumerprops.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        consumerprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autocomm);
        consumerprops.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        consumerprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, num_records);
    }

    /**
     * Get Kafka message key
     *
     * @return
     */
    public ArrayList<String> get_msgkey() {
        return msg_keys;
    }

    /**
     * Get Kafka message value
     *
     * @return
     */
    public ArrayList<String> get_msgvalue() {
        return msg_values;
    }

    /**
     * Get records for a Kafka consumer
     *
     * @param topic       - Kafka topic
     * @param poll_time   - time interval for polling [sec.]
     * @param num_records - maximum number of records per poll operation [1]
     * @throws JSONException
     */
    public void get_records(String topic, int poll_time) throws JSONException {

        // Start Kafka consumer and subscribe to topic
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerprops);
        consumer.subscribe(Collections.singletonList(topic));

        // Iterate through messages
        try {

            ConsumerRecords<String, String> consumerrec = consumer.poll(Duration.ofSeconds(poll_time));
            if (consumerrec.count() == 0) {
                System.out.println("No Record!");
            } else {
                System.out.println(consumerrec.count() + " records found!");
            }

            // Extract key and value messages
            consumerrec.forEach(record -> {

                // Update message value and key
                msg_keys.add(record.key());
                msg_values.add(record.value());

                System.out.println(record.value());

            });

            // Commit offset
            if ((boolean) consumerprops.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
                consumer.commitSync();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }

    /**
     * Send topics with a Kafka producer
     *
     * @param topic - Kafka topic
     * @param key   - Kafka record key
     * @param value - Kafka record value
     */
    public void send_records(String topic, String key, String value) {

        // Set serialization properties
        producerprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Start producer
        set_producerproperties();
        Producer<String, String> producer = new KafkaProducer<String, String>(producerprops);

        try {

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            producer.send(record).get();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }

    }

}
