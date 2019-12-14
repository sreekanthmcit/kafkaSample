package com.sreekanth.kafka.firsttutorial;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
	
	public static void main(String[] args) {
		
		String bootstrapServers = "localhost:9092";
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
		
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "First Java Message send");
		
		
		
		kafkaProducer.send(producerRecord);
		
		kafkaProducer.flush();
		kafkaProducer.close();
		
	}

}
