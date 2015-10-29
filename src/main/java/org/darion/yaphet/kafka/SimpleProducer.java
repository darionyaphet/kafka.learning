package org.darion.yaphet.kafka;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import com.google.common.io.Files;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {
	private static final String RESOURCE = "resource/hamlet.txt";
	// private static final Logger LOG = Logger.getLogger(Simple.class);

	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("metadata.broker.list", "localhost:9092");

		ProducerConfig config = new ProducerConfig(properties);
		Producer<String, String> producer = new Producer<String, String>(config);

		List<String> lines = Files.readLines(new File(RESOURCE), Charset.defaultCharset());
		for (String line : lines) {
			producer.send(new KeyedMessage<String, String>("topic.default", line));
		}

		producer.close();
	}
}
