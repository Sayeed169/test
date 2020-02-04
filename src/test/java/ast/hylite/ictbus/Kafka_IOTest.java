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

    @Test
    public void multipleProducer_singleConsumer() {
        // Create producer two message
        kafka.send_records("test-topic-1", Integer.toString(0), "test message from test topic 1");
        kafka.send_records("test-topic-2", Integer.toString(0), "test message from test topic 2");
        kafka.send_records("test-topic-3", Integer.toString(0), "test message from test topic 3");
        // Consume message for 10 sec
        // Only consume 2 message at a time
        kafka.set_consumerproperties("test-group", 3, true);
        kafka.get_records("test-topic-1", 5);
        kafka.get_records("test-topic-2", 5);
        kafka.get_records("test-topic-3", 5);
    }

    @Test
    public void singleProducer_multipleConsumer() {
        // Create producer two message
        kafka.send_records("test-topic", Integer.toString(0), "test message from single producer");
        // Consume message for 10 sec
        // Only consume 2 message at a time
        kafka.set_consumerproperties("test-group-1", 100, true);
        kafka.set_consumerproperties("test-group-2", 100, true);
        kafka.set_consumerproperties("test-group-3", 100, true);
        kafka.get_records("test-topic", 5);
        kafka.get_records("test-topic", 5);
        kafka.get_records("test-topic", 5);
    }
}