package ast.hylite.ictbus;

import org.json.JSONException;

import ast.hylite.ictbus.Kafka_IO;

public class Test_ProducerConsumer {  
		
	// Main method
	public static void main(String[] args) {
		
		// Declarations
		String bootstrapservers = "localhost:9092";
		String gettopic = "test2";
		String sendtopic = "test2";
		
		// Initialization
		Kafka_IO kafka_io = new Kafka_IO(bootstrapservers);
		
		// Set Kafka producer and consumer
		kafka_io.set_consumerproperties("test", 10, Boolean.TRUE);
		
		
		
		// Send records
		
		//for(int i=0;i<10;i++) {
		//	kafka_io.send_records(sendtopic, "test_key", "test_value" + String.valueOf(i));
			
		//}
		
		
		
		// Get records
		try {
			kafka_io.get_records(gettopic, 100000);
						
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
