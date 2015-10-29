package org.darion.yaphet.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SimpleConsumer {
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "");
		properties.put("group.id", "");
		properties.put("zookeeper.session.timeout.ms", "400");
		properties.put("zookeeper.sync.time.ms", "200");
		properties.put("auto.commit.interval.ms", "1000");

		ConsumerConfig config = new ConsumerConfig(properties);
		Map<String, Integer> topicCountMap = Maps.newHashMap();
		topicCountMap.put("", 1);

		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get("").get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext())
			System.out.println(new String(it.next().message()));
	}

}
