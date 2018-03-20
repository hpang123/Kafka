package hpang.kafka.producer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountryPartitioner implements Partitioner {

	private static final Logger log = LoggerFactory.getLogger(CountryPartitioner.class);

	private static Map<String, Integer> countryToPartitionMap;

	/*
	 *When Kafka calls configure(), the Kafka producer will pass all the properties 
	 *that we've configured for the producer to the Partitioner class. 
	 */
	public void configure(Map<String, ?> configs) {
		log.info("configure: " + configs);

		countryToPartitionMap = new HashMap<String, Integer>();
		for (Map.Entry<String, ?> entry : configs.entrySet()) {
			if (entry.getKey().startsWith("partitions.")) {
				String keyName = entry.getKey();
				String value = (String) entry.getValue();

				log.info(keyName.substring(11));
				// partitions.n, partionId will be n
				int paritionId = Integer.parseInt(keyName.substring(11));
				countryToPartitionMap.put(value, paritionId);
			}
		}
		log.info("countryToPartitionMap:" + countryToPartitionMap);
	}
	
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
		
		//the value is format like countryname:xxxx
		String countryName = ((String) value).split(":")[0];
		log.info("country name:" + countryName);
		if (countryToPartitionMap.containsKey(countryName)) {
			// If the country is mapped to particular partition return it
			return countryToPartitionMap.get(countryName);
		} else {
			/* If no country is mapped to particular partition, 
			 * distribute between remaining partitions
			 */
			int noOfPartitions = partitions.size();
			return countryName.hashCode() % (noOfPartitions - countryToPartitionMap.size());
		}
	}
	
	public void close() {}
}
