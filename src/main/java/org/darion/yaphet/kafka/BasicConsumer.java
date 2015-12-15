package org.darion.yaphet.kafka;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BasicConsumer {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

		final String topic = "";
		KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Collections.singletonList(topic));

		ConsumerRecords<Integer, String> records = consumer.poll(1000);
		for (ConsumerRecord<Integer, String> record : records) {
			System.out.println(
					"Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
		}

		consumer.close();
	}
}
