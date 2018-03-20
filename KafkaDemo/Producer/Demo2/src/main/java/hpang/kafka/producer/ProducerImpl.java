package hpang.kafka.producer;

import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerImpl implements Producer {
	private static final Logger log = LoggerFactory.getLogger(ProducerImpl.class);

	private class ProducerCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			if (e != null) {
				e.printStackTrace();
			}
			else{
				log.info("Topic: {}, Partition: {}, Offset:{}", 
						recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
			}
		}
	}

	@Override
	public void start(MODE mode) {

		Properties producerProps = new Properties();
		/*
		 * List of host:port pairs of brokers that the producer will use to
		 * establish initial connection to the Kafka cluster(at least two)
		 */
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		/* Pass the customize partitioner */
		producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CountryPartitioner.class.getName());
		/* these properties will pass to config in Partitoner class */
		producerProps.put("partitions.8", "US");
		producerProps.put("partitions.9", "BR");
		
				
		/*
		 * controls how many partition replicas must receive the record before
		 * the producer can consider the write successful
		 */
		producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
		producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);
		producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);
		/*
		 * linger.ms controls the amount of time to wait for additional messages
		 * before sending the current batch. KafkaProducer sends a batch of
		 * messages either when the current batch is full or when the linger.ms
		 * limit is reached. By default, the producer will send messages as soon
		 * as there is a sender thread available to send them, even if there’s
		 * just one message in the batch.
		 */
		producerProps.put("linger.ms", 1);
		/*
		 * amount of memory the producer will use to buffer messages waiting to
		 * be sent to brokers.
		 */
		producerProps.put("buffer.memory", 24568545);
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);

		for (int i = 0; i < 2000; i++) {
			/*
			 * We start producing messages to Kafka by creating a
			 * ProducerRecord, which must include the topic we want to send the
			 * record to and a value. Optionally, we can also specify a key
			 * and/or a partition. If we specified a partition in the
			 * ProducerRecord, the partitioner doesn’t do anything and simply
			 * returns the specified partition. If we didn’t, the partitioner
			 * will choose a partition for us, usually based on the
			 * ProducerRecord key.
			 */

			Random rnd = new Random();
	        int countries = Locale.getISOCountries().length;
	        String country = Locale.getISOCountries()[rnd.nextInt(countries)];

	        String key = "message" + i;
			String message = country + ":" + rnd.nextDouble();
			
			log.info("key:{} message:{}", key, message);
			
			ProducerRecord<String, String> data = new ProducerRecord<String, String>("test-topic", key, message);

			/*
			 * RecordMetadata has the topic, partition, and the offset of the
			 * record within the partition.
			 * 
			 * When the producer receives an error, it may retry sending the
			 * message a few more times before giving up
			 */
			try {
				if (i % 15 == 0) {
					/*
					 * For test batch size and linger.ms
					 */
					Thread.sleep(1000);
				}

				switch (mode) {
				case IGNORE:
					//Fire and forget mode
					producer.send(data);
					break;
				case SYSNCHRONIZE:
					Future<RecordMetadata> recordMetadataFuture = producer.send(data);
					RecordMetadata recordMetadata = recordMetadataFuture.get();
					log.info("Topic: {}, Partition: {}, Offset:{a}", recordMetadata.topic(), recordMetadata.partition(),
							recordMetadata.offset());
					break;
				case ASYSNCHRONIZE:
					producer.send(data, new ProducerCallback());	

				}

			} catch (Exception e) {
				/*
				 * KafkaProducer has two types of errors: Retriable(connection,
				 * no leader) and Non-Retriable(message size too large)
				 * Retriable errors are those that can be resolved by sending
				 * the message again. KafkaProducer can be configured to retry
				 * those errors automatically, so the application code will get
				 * retriable exceptions only when the number of retries was
				 * exhausted and the error was not resolved.
				 */
				e.printStackTrace();
			}
			/*
			 * Once we send the ProducerRecord, the first thing the producer
			 * will do is serialize the key and value objects to ByteArrays so
			 * they can be sent over the network.
			 * 
			 * It then adds the record to a batch of records that will also be
			 * sent to the same topic and partition. A separate thread is
			 * responsible for sending those batches of records to the
			 * appropriate Kafka brokers.
			 */
			log.info("Message: key={}, value={}", data.key(), data.value());
		}
		producer.close();

		// log.info("Send " +mail);

	}
}
