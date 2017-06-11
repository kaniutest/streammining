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
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
import scala.None;
import twitter4j.Status;

import org.elasticsearch.spark.rdd.EsSpark ;

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
			log.error(e);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static final long serialVersionUID = 1L;
	private static final String APP_NAME = "twitterstreamer";
	private static final String KFK_OUT_TOPIC = "tweet-topic";
	
	@SuppressWarnings("unused")
	private SparkTweetStreamer(){}
	
	private int threads = 1;
	private String kafkaouttopic = "tweet-topic";
	
	
	
	public SparkTweetStreamer(String arg1, int _threads, String _kafkaouttopic){
		
		threads = _threads;
		kafkaouttopic = _kafkaouttopic;
	}


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
			} catch (JsonGenerationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
	
	
	private void writeToKafka(byte[] message, Properties props){
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);
		Future<RecordMetadata> response = producer.send(new ProducerRecord<String, byte[]>(KFK_OUT_TOPIC, message));
		producer.close();
	}
	
	
	
	
	
	
}
