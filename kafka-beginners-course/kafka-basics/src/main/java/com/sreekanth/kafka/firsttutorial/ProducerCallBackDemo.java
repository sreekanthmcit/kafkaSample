package com.sreekanth.kafka.firsttutorial;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallBackDemo {

	public static final Logger logger = LoggerFactory.getLogger(ProducerCallBackDemo.class);

	public static void main(String[] args) {

		String bootstrapServers = "localhost:9092";

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

		for (int messageCounter = 0; messageCounter < 10; messageCounter++) {

			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic",
					"First Java Message send" + messageCounter);

			kafkaProducer.send(producerRecord, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {

					if (exception == null) {
						logger.info("Logging info \n" + "\n Topic :" + metadata.topic() + "\n Partition: "
								+ metadata.partition() + "\n offesets: " + metadata.offset() + "\n Timestamp: "
								+ metadata.timestamp());
					}

				}
			});
		}

		kafkaProducer.flush();
		kafkaProducer.close();

	}

}
