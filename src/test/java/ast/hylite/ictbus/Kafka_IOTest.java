package ast.hylite.ictbus;

import static org.junit.Assert.*;

public class Kafka_IOTest {

    private Kafka_IO kafka;

    @org.junit.Before
    public void setUp() throws Exception {
        kafka = new Kafka_IO("localhost:9092");
    }

    @org.junit.After
    public void tearDown() throws Exception {
        kafka = null;
    }

    @org.junit.Test
    public void get_records() {
        kafka.set_consumerproperties("test-group", 10, true);
        kafka.get_records("test", 0);
    }

    @org.junit.Test
    public void send_records() {
        kafka.send_records("devglan-test", Integer.toString(0), "test message");
    }
}