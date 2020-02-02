package ast.hylite.ictbus;

import java.util.Properties;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

public class Test_KafkaAdmin {  
		
	/**
	Sources: 
	https://www.programcreek.com/java-api-examples/index.php?api=kafka.server.KafkaServer
	https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm
	https://www.devglan.com/apache-kafka/apache-kafka-java-example
	http://cloudurable.com/blog/kafka-tutorial-kafka-producer/index.html
	http://cloudurable.com/blog/kafka-tutorial-kafka-consumer/index.html
	**/
	
	Test_KafkaAdmin(String bootstrapservers) {
		setScripts();
	}
	
	// Declarations
	static Properties producerprops = new Properties();
	static Properties consumerprops = new Properties();
	public String kafka_dir;
	public String kafkamanager_dir;
	public String startzooscript;
	public String startzoopara;
	public String startkafkascript;
	public String startkafkapara;
	public String stopkafkascript;
	public String startkafkaconnect;
	public String startkafkaconnectpara;
	public String startkafkamanager;
	public String startkafkamanagerpara;
	public String settopicscript;
	public String settopicpara;
	
	// Set shell script configuration
	private void setScripts() {
		
		kafka_dir = "D:\\Projects\\ENV\\kafka_2.11-2.4.0";
		kafkamanager_dir = "/opt/kafka-manager-master/target/universal/kafka-manager-1.3.3.22";
		startzooscript = "bin/zookeeper-server-start.sh";
		startzoopara = "config/zookeeper.properties";
		startkafkascript = "bin/kafka-server-start.sh";
		startkafkapara = "config/server.properties";
		stopkafkascript = "bin/kafka-server-stop.sh";
		startkafkaconnect = "bin/connect-standalone.sh";
		startkafkaconnectpara = "config/connect-standalone.properties config/mysql-sink-requests.properties";
		startkafkamanager = "bin/kafka-manager";
		startkafkamanagerpara = "-Dkafka-manager.zkhosts=\"localhost:2181\"";
		settopicscript = "bin/kafka-topics.sh";
		settopicpara = "--create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test";
		
	}
		
	// Execute shell scripts
	public class ShellCommands implements Runnable{

		private String command;
		private String paras;
		private String workdir;
		private Boolean verbose;
		
		public ShellCommands(String command, String paras, String workdir, Boolean verbose) {
			this.command = command;
			this.paras = paras;
			this.workdir = workdir;
			this.verbose = verbose;
			
		}
		
		@Override
		public void run() {
			
			try {
				
				ProcessBuilder pb = new ProcessBuilder(this.command,this.paras);
				pb.directory(new File(this.workdir));
				Process p = pb.start();
				
				if(this.verbose) {
					BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
					
					String line;
					while((line = reader.readLine()) != null) {
						System.out.println(line);
					}
				}
				
			} catch(Exception e) {
				e.printStackTrace();
			}
			
		}
		
	}
		
	// Main method
	public static void main(String[] args) {
		
		// Declarations
		String bootstrapservers = "localhost:9092";
		
		// Initialize
		Test_KafkaAdmin test = new Test_KafkaAdmin(bootstrapservers);
		
		// Start Zookeeper and Kafka server
		//Thread zoostart = new Thread(test.new ShellCommands(test.startzooscript, test.startzoopara, test.kafka_dir, Boolean.TRUE));
		//zoostart.start();
		//Thread kafkastart = new Thread(test.new ShellCommands(test.startkafkascript, test.startkafkapara, test.kafka_dir, Boolean.TRUE));
		//kafkastart.start();
		//Start Kafka manager
		Thread kafkamanagerstart = new Thread(test.new ShellCommands(test.startkafkamanager, "", test.kafkamanager_dir, Boolean.TRUE));
		kafkamanagerstart.start();
		
		// Start kafka connect
		//Thread connectstart = new Thread(test.new ShellCommands(test.startkafkaconnect, test.startkafkaconnectpara, test.kafka_dir, Boolean.TRUE));
		//connectstart.start();
		
		// Create topic
		//Thread kafkatopic = new Thread(test.new ShellCommands(test.settopicscript, test.settopicpara, test.kafka_dir, Boolean.TRUE));
		//kafkatopic.start();
				
		// Stop Kafka server
		//Thread kafkastop = new Thread(test.new ShellCommands(test.stopkafkascript, "", test.kafka_dir, Boolean.TRUE));
		//kafkastop.start();

	}

}
