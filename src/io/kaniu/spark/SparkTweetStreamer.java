package io.kaniu.spark;


import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.codehaus.jackson.map.ObjectMapper;

import twitter4j.Status;



/**
 * This class reads from twitter feed and writes to kafka topic.
 * 
 * */
public class SparkTweetStreamer implements Serializable{
	static final Logger log = Logger.getLogger(SparkTweetStreamer.class);
	
	/**
	 * 
	 */
	static private Properties kafkaprops = null;
	static{
		kafkaprops = new Properties();
		InputStream producerprops = SparkTweetStreamer.class.getClassLoader().getResourceAsStream("kafkaproducer.properties");
		try {
			kafkaprops.load(producerprops);
		} catch (IOException e) {
			log.error(e,e);
			e.printStackTrace();
		}
	}
	private static final long serialVersionUID = 1L;
	private static final String APP_NAME = "twitterstreamer";
	
	@SuppressWarnings("unused")
	private SparkTweetStreamer(){}
	
	private int threads = 1;
	private String kafkaouttopic = null;
	
	
	
	public SparkTweetStreamer(String arg1, int _threads, String _kafkaouttopic){
		
		threads = _threads;
		kafkaouttopic = _kafkaouttopic;
	}

	/**
	 * Filters out message without Geolocation then writes to kafka topic for better loadbalancing downstream. 
	 * */
	public void run() {
		
		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.driver.allowMultipleContexts","true");
		
		  if (!sparkConf.contains("spark.master")) {
		      sparkConf.setMaster("local[2]");
		    }
		  
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
	    JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc); 
	    
	    JavaDStream<Status> filteredtwt =  tweets.filter(  new Function<Status, Boolean>() {
            public Boolean call(Status status){
                if (status.getGeoLocation() != null) {
                    return true;
                } else {
                    return true;
                }
            }
        });
	    
	    JavaDStream<String> words = filteredtwt.map(new Function<Status, String>() {
	    	private static final long serialVersionUID = 1L;
	      @Override
	      public String call(Status s) {
	    	  
	    	  ObjectMapper mapper = new ObjectMapper();
	    	  String ret = "";
	    	  try {
	    		  ret = mapper.writeValueAsString(s);
			} catch (Exception e) {
				log.error(e,e);
			} 
	    	  return ret;

	      }
	    });
	    
	    words.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<String> rdd, Time arg1) throws Exception {
				
				rdd.foreach(new VoidFunction<String>(){
					final Properties kfk_props = kafkaprops;
					private static final long serialVersionUID = 1L;

					@Override
					public void call(String str) throws Exception {
						System.out.println(str);
						writeToKafka(str.getBytes(), kfk_props);
						return;
					}});
				
				return null;
			}
		});
	    
		jssc.start();
		try {
		      jssc.awaitTermination();
		    } catch (Throwable e) {
		      e.printStackTrace();
		    }
	}
	
	
	/**
	 * Kafka producer function. 
	 * 
	 * @param message
	 * @param props
	 */
	private void writeToKafka(byte[] message, Properties props){
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
		Future<RecordMetadata> response = producer.send(new ProducerRecord<String, byte[]>(kafkaouttopic, message));
		producer.close();
	}
	
	
	
	
	
}
