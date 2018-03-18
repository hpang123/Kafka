package hpang.kafka.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/*
 * Start zookeeper server
 * \zookeeper-3.4.10\bin\zkserver
 * 
 * Start Kafka:
 * \kafka_2.12-1.0.1:
 * .\bin\windows\kafka-server-start.bat .\config\server.properties
 * 
 * To run:
 * mvn spring-boot:run
 * Or package to jar:
 * mvn clean package
 */

@SpringBootApplication
public class ProducerDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProducerDemoApplication.class, args);
	}
	
	@Bean
	public ProducerRunner producerRunner() {
		return new ProducerRunner();
	}
	
	@Bean
	public Producer producer() {
		return new ProducerImpl();
	}
}
