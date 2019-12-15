package com.sreekanth.kafka.firsttutorial;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoThread {
	private final Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class);

	public ConsumerDemoThread() {

	}

	public void run() {
		String topic = "first_topic";
		String groupId = "sixthGroup";
		String bootstrapServer = "localhost:9092";
		CountDownLatch latch = new CountDownLatch(1);
		logger.info("Creating the consumer");
		Runnable consumerRunnable = new ConsumerRunnable(topic, bootstrapServer, groupId, latch);

		Thread myThread = new Thread(consumerRunnable);
		myThread.start();
		
		//Add ShutDown hook
		
		Runtime.getRuntime().addShutdownHook(new Thread(()-> {
			logger.info("Caught Shutdown hook");
			((ConsumerRunnable) consumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("Application Interrupted" + e );
		} finally {
			logger.info("Application closing" );
		}

	}

	public static void main(String[] args) {
		new ConsumerDemoThread().run();
	}

	public class ConsumerRunnable implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

		public ConsumerRunnable(String topic, String bootstrapServer, String groupId, CountDownLatch latch) {
			this.latch = latch;
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			this.consumer = new KafkaConsumer<String, String>(properties);
			consumer.subscribe(Arrays.asList(topic));
		}

		public void run() {
			try {
				while (true) {
					ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

					for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
						logger.info("key: " + consumerRecord.key());
						logger.info("key: " + consumerRecord.value());
						logger.info("offset: " + consumerRecord.offset());
						logger.info("Partition: " + consumerRecord.partition());
					}

				}
			} catch (WakeupException e) {
				logger.info("Received Shutdown Signal");
			} finally {
				consumer.close();
				// Tell Main Method that we are done with consumer
				latch.countDown();
			}

		}

		public void shutdown() {
			// interuppt the consumer from polling will throw wakeupException
			consumer.wakeup();
		}

	}

}
