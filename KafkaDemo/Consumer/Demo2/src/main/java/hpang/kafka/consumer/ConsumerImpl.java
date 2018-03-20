package hpang.kafka.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * You can’t have multiple consumers that belong to the same group
 * in one thread and you can’t have multiple threads safely use the
 * same consumer. One consumer per thread is the rule.
 */
public class ConsumerImpl implements Consumer {
	private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

	@Override
	public void start() {
		/*
		 * Or use Collections.singletonList("test-topic") for single topic
		 */
		String topic = "test-topic";
		List<String> topicList = new ArrayList<>();
		topicList.add(topic);

		Properties consumerProperties = new Properties();
		/*
		 * It can define multiple brokers like this: "broker1:9092,broker2:9092"
		 */
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		/*
		 * When multiple consumers are subscribed to a topic and belong to the
		 * same consumer group, each consumer in the group will receive messages
		 * from a different subset of the partitions in the topic. If we add
		 * more consumers to a single group with a single topic than we have
		 * partitions, some of the consumers will be idle and get no messages at
		 * all.
		 * 
		 * Consumers in different groups get messages independently Create a new
		 * consumer group for each application that needs all the messages from
		 * one or more topics. Add consumers to an existing consumer group to
		 * scale the reading and processing of messages from the topics, so each
		 * additional consumer in a group will only get a subset of the
		 * messages.
		 */
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "demogroup1");
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		/*
		 * Set it to false if you prefer to control when offsets are committed,
		 * which is necessary to minimize duplicates and avoid missing data. If
		 * you set enable.auto.commit to true, then you might also want to
		 * control how frequently offsets will be committed using
		 * auto.commit.interval.ms.
		 * 
		 * Whenever you poll, the consumer checks if it is time to commit, and
		 * if it is, it will commit the offsets it returned in the last poll.
		 */
		consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		consumerProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		/*
		 * The amount of time a consumer can be out of contact with the brokers
		 * while still considered alive defaults to 3 seconds.
		 * 
		 * heatbeat.interval.ms must be lower than session.timeout.ms, and is
		 * usually set to one-third of the timeout value
		 */
		consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);

		/*
		 * We can also call subscribe with a regular expression:
		 * consumer.subscribe("test.*"); Most commonly used in applications that
		 * replicate data between Kafka and another system.
		 */
		consumer.subscribe(topicList);
		log.info("Subscribed to topic " + topic);

		Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
		int count = 0;

		try {
			while (true) {
				/*
				 * consumers must keep polling Kafka or they will be considered
				 * dead and the partitions they are consuming will be handed to
				 * another consumer in the group to continue consuming.
				 * 
				 * pass timeout interval param to poll
				 */
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					log.info("topic={}, partition={}, offset={}, key ={}, value={}", record.topic(), record.partition(),
							record.offset(), record.key(), record.value());

					// do something with record...

					/*
					 * After reading each record, we update the offsets map 
					 * with the offset of the next message we expect to process. 
					 * This is where we’ll start reading next time we start.
					 */
					currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
							new OffsetAndMetadata(record.offset() + 1, "no metadata"));
					
					// Commmit every 20 records
					if (count % 20 == 0) {
						// pass a map of partitions and offsets that you wish to commit.
						consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
							public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
								if (e != null) {
									log.error("Commit failed for offsets {} due to {}", offsets, e);
								}
								// log.info("Commit offsets {}", offsets);
							}
						});
					}
					count++;
				}
			}
		} catch (Exception ex) {
			// TODO : Log Exception Here
			log.error("Unexpected error", ex);
		} finally {
			try {
				/*
				 * if we know that this is the last commit before we close the
				 * consumer, or before a rebalance, we want to make extra sure
				 * that the commit succeeds. So use commitSync() here commit the
				 * latest offset returned by poll() and return once the offset
				 * is committed. commitSync() will retry the commit until it
				 * either succeeds or encounters a nonretriable failure,
				 */
				consumer.commitSync();

			} finally {
				/*
				 * Always close() the consumer before exiting. This will close
				 * the network connections and sockets. It will also trigger a
				 * rebalance immediately rather than wait for the group
				 * coordinator to discover that the consumer stopped sending
				 * heartbeats and is likely dead,
				 */
				consumer.close();
			}
		}
	}

}
