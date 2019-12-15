package com.sreekanth.kafka.firsttutorial;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignAndSeek {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);
	
	public static void main(String[] args) {
		String topic = "first_topic";
		String bootstrapServer = "localhost:9092";
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		TopicPartition partitionstoReadFrom = new TopicPartition(topic, 0);
		Long offsetsToReadFrom =10L;
		
		//Assign
		consumer.assign(Arrays.asList(partitionstoReadFrom));
		
		//Seek
		consumer.seek(partitionstoReadFrom, offsetsToReadFrom);
		
		int noOfMessagesToRead = 5;
		boolean keepReading = true;
		int noOfMessagesReadSoFar = 0;
		
		
		while(keepReading) {
			ConsumerRecords<String, String>consumerRecords =  consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				noOfMessagesReadSoFar +=1;
				logger.info("key: " + consumerRecord.key());
				logger.info("key: " + consumerRecord.value());
				logger.info("offset: " + consumerRecord.offset());
				logger.info("Partition: " + consumerRecord.partition());
				if(noOfMessagesReadSoFar>noOfMessagesToRead) {
					keepReading = false;
					break;
				}
			}
			
		}
		
	}

}
