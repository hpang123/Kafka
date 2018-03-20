package hpang.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

public class ConsumerRunner implements CommandLineRunner {
	private static final Logger log = LoggerFactory.getLogger(ConsumerRunner.class);

	@Autowired
	Consumer consumer;
	
	@Override
	public void run(String... strings) throws Exception {
		log.info("Kafka consumer start...");
		consumer.start();
	}
}
