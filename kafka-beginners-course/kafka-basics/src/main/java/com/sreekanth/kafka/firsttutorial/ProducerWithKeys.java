package com.sreekanth.kafka.firsttutorial;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeys {

	public static final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		String bootstrapServers = "localhost:9092";

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

		for (int messageCounter = 0; messageCounter < 10; messageCounter++) {

			String topic = "first_topic";
			String message = " Java Messages send" + messageCounter;
			String key = "key_" + messageCounter;
			
			logger.info(key);

			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, message);

			kafkaProducer.send(producerRecord, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {

					if (exception == null) {
						logger.info("Logging info \n" + "\n Topic :" + metadata.topic() + "\n Partition: "
								+ metadata.partition() + "\n offesets: " + metadata.offset() + "\n Timestamp: "
								+ metadata.timestamp());
					}

				}
			}).get();
		}

		kafkaProducer.flush();
		kafkaProducer.close();

	}

}
