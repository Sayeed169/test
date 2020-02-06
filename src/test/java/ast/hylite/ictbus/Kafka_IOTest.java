package ast.hylite.ictbus;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Kafka_IOTest {

    private Kafka_IO kafka;

    @Before
    public void setUp() throws Exception {
        kafka = new Kafka_IO("localhost:9092");
    }

    @After
    public void tearDown() throws Exception {
        kafka = null;
    }

    @Test
    public void send_records() {
        kafka.send_records("test-topic", Integer.toString(0), "test message");
        assertTrue("Send record success", true);
    }

    @Test
    public void get_records() {
        kafka.set_consumerproperties("test-group", 1, true);
        kafka.get_records("test-topic", 5);
        assertTrue("Consume record success", true);
    }

    @Test
    public void singleProducer_singleConsumer() {
        // Create producer two message
        kafka.send_records("test-topic", Integer.toString(0), "test message 1");
        kafka.send_records("test-topic", Integer.toString(1), "test message 2");
        // Consume message for 10 sec
        // Only consume 2 message at a time
        kafka.set_consumerproperties("test-group", 2, true);
        kafka.get_records("test-topic", 10);

    }

    /**
     * Multiple producer produce message in a single topic, Single consumer
     */
    @Test
    public void multipleProducer_singleConsumer() {
        Kafka_IO kafka_1 = new Kafka_IO("localhost:9092");
        Kafka_IO kafka_2 = new Kafka_IO("localhost:9092");
        Kafka_IO kafka_3 = new Kafka_IO("localhost:9092");
        // Create message from producer
        kafka_1.send_records("test-topic", Integer.toString(0), "test message from Producer 1");
        kafka_2.send_records("test-topic", Integer.toString(0), "test message from Producer 2");
        kafka_3.send_records("test-topic", Integer.toString(0), "test message from Producer 3");
        // Consume message for 10 sec
        // Only consume 2 message at a time
        kafka.set_consumerproperties("test-group", 5, true);
        kafka.get_records("test-topic", 5);
    }

    /**
     * Precondition - Create 3 brocker using
     * 
     * @see docker exec -it {kafka-server-name} kafka-topics --zookeeper
     *      zookeeper:2181 --create --topic multipleBroker \ --partitions 1
     *      --replication-factor 3
     */
    @Test
    public void singleProducer_multipleConsumer() {
        // Create producer two message
        kafka.send_records("multipleBroker", Integer.toString(0), "1st message from single producer");
        kafka.send_records("multipleBroker", Integer.toString(0), "2nd message from single producer");
        kafka.send_records("multipleBroker", Integer.toString(0), "3rd message from single producer");

        // Consume message for 10 sec
        // Only consume 2 message at a time
        Kafka_IO kafka_1 = new Kafka_IO("localhost:9091");
        Kafka_IO kafka_2 = new Kafka_IO("localhost:9092");
        Kafka_IO kafka_3 = new Kafka_IO("localhost:9093");

        kafka_1.set_consumerproperties("test-group", 3, true);
        // kafka_2.set_consumerproperties("test-group", 1, true);
        // kafka_3.set_consumerproperties("test-group", 3, true);

        kafka_1.get_records("multipleBroker", 5);
        kafka_2.get_records("multipleBroker", 5);
        kafka_3.get_records("multipleBroker", 5);
    }
}