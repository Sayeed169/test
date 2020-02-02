package ast.hylite.ictbus;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class Producer {

    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Scanner sc = new Scanner(System.in);

        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        try{
            // Publish Test Messages at once
//            for(int i = 0; i < 100; i++){
//                System.out.println(i);
//                kafkaProducer.send(new ProducerRecord("devglan-test", Integer.toString(i), "test message - " + i ));
//            }
            // Publish Test Messages on promt
            while(true){
                String str = sc.nextLine();
                System.out.println("Message from Producer: "+str);
                kafkaProducer.send(new ProducerRecord("devglan-test", Integer.toString(0), "test message - " + str ));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}