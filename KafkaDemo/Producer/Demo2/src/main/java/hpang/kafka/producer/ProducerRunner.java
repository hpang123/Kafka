package hpang.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

import hpang.kafka.producer.Producer.MODE;

public class ProducerRunner implements CommandLineRunner {
	private static final Logger log = LoggerFactory.getLogger(ProducerRunner.class);

	@Autowired
	Producer producer;
	
	@Override
	public void run(String... strings) throws Exception {
		log.info("Kafka producer start...");
		/*
		 * MODE can be MODE.IGNORE, MODE.ASYNCHRONIZE, MODE,SYSNCHRONIZE
		 */
		producer.start(MODE.ASYSNCHRONIZE);
	}
}
