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
        kafka.get_records("", 0);
    }

    @org.junit.Test
    public void send_records() {
    }
}