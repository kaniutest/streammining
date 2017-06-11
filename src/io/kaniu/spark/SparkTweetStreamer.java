package io.kaniu.spark;


import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
import scala.None;
import twitter4j.Status;



public class SparkTweetStreamer {
	
	String APP_NAME = "twitterstreamer";
	
	@SuppressWarnings("unused")
	private SparkTweetStreamer(){}
	
	private int threads = 1;
	private String kafkaouttopic = null;
	
	private Properties kafkaprops = null;
	
	public SparkTweetStreamer(String arg1, int _threads, String _kafkaouttopic){
		
		threads = _threads;
		kafkaouttopic = _kafkaouttopic;
	}


	public void run() {
		
		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.driver.allowMultipleContexts","true");
		
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
	    JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc); //TwitterUtils.createStream(jssc, auth);

	    JavaDStream<String> words = tweets.flatMap(new FlatMapFunction<Status, String>() {
	    	private static final long serialVersionUID = 1L;
	      @Override
	      public Iterable<String> call(Status s) {
	        return (Iterable<String>) Arrays.asList(s.getText().split(" ")).iterator();
	      }
	    });
	    
		jssc.start();
		jssc.awaitTermination();
	}
	
	
	private void writeToKafka(byte[] message, Properties props){
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);// retrieveCachedPooledKafkaProducer(props);//retrieveCachedKafkaProducer(props);
		Future<RecordMetadata> response = producer.send(new ProducerRecord<String, byte[]>(props.getProperty("topic.id"), message));
		producer.close();
	}
	
	
	
	
	
	
}
