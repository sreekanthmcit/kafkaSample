package com.sreekanth.kafka.secondtutorial;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.sreekanth.kafka.firsttutorial.ConsumerDemoThread.ConsumerRunnable;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
	String consumerKey = "tzrZbqu05i3BcGsXcLcTOMkUt";
	String consumerSecret = "kbA1QZwl8Hk04owQNYf5f5bMgOxUeBMtwC9X2lCZmGvHCVc4hp";
	String accessToken = "54934738-B7bHRcNzRVKtKw7zqagPIKfu9MoeQ4rFnpWnI70u1";
	String accessTokenSecret = "LkfBcQ9asvULshxM1yNm0qn09mgwwEn1Cqo7FZGw5zwa0";
	

	public TwitterProducer() {

	}

	public static void main(String[] args) {
		System.out.println("Hello Twitter");
		new TwitterProducer().run();

	}

	public void run() {
		// Twitter client
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		// Attempts to establish a connection.
		Client client = createTwitterClient(msgQueue);
		client.connect();
		
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		Runtime.getRuntime().addShutdownHook(new Thread(()-> {
			logger.info("Caught Shutdown hook");
			client.stop();
			producer.close();
		}));

		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}
			if(msg!=null) {
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets",null, msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception!=null) {
							logger.error("Something was not right");
						}
						
					}
				});
			}
		}
		logger.info("End of Application");

		// KafkaProducer

		// sendTweets
	}

	

	private KafkaProducer<String, String> createKafkaProducer() {
		String bootstrapServers = "localhost:9092";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return new KafkaProducer<String, String>(properties);
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		// BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("bitcoin");
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.

		return hosebirdClient;
	}

}
