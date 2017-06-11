package io.kaniu.spark;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkSentimentalAnalysis {
	
	static final Logger log = Logger.getLogger(SparkSentimentalAnalysis.class);
	
	static private Properties consumerProperties = null;
	static{
		consumerProperties = new Properties();
		InputStream producerprops = SparkTweetStreamer.class.getClassLoader().getResourceAsStream("kafkaconsumer.properties");
		try {
			consumerProperties.load(producerprops);
		} catch (IOException e) {
			log.error(e);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static final long serialVersionUID = 1L;
	private static final String APP_NAME = "twitterstreamer";
	private static final String KFK_OUT_TOPIC = "tweet-topic";
	
	private String kafkainputTopic = "processed-topic";
	private int numThreads = 1;
	
	private SparkSentimentalAnalysis(){}
	
	public SparkSentimentalAnalysis(String _kafkainputTopic, int _numThreads){
		kafkainputTopic = _kafkainputTopic;
	}
	
	public void run() throws IOException{

		//StreamingExamples.setStreamingLogLevels();
		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.driver.allowMultipleContexts","true");
		  
		if (!sparkConf.contains("spark.master")) {
		      sparkConf.setMaster("local[2]");
		 }
		
		// Create the context with 500 milliseconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(500));

		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = kafkainputTopic.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}


		  
		// Create direct kafka stream with brokers and topics
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", consumerProperties.getProperty("bootstrap.servers"));

		JavaPairInputDStream<String, byte[]> messages = KafkaUtils.createDirectStream(
				jssc,
				String.class,
				byte[].class,
				StringDecoder.class,
				DefaultDecoder.class,
				kafkaParams,
				topicsSet
				);

		messages.foreachRDD(new Function<JavaPairRDD<String, byte[]>, Void>() {
			private static final long serialVersionUID = 1L;
			//String daily_hdfsfilename = new SimpleDateFormat("yyyyMMdd").format(new Date());

			@Override
			public Void call(JavaPairRDD<String, byte[]> rdd) throws IOException, ClassNotFoundException, SQLException {
				
				rdd.foreachPartitionAsync( new VoidFunction<Iterator<Tuple2<String, byte[]>>>(){
					
					
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, byte[]>> itTuple) throws Exception {
						
						while(itTuple.hasNext()){

							Tuple2<String, byte[]> tuple2  = itTuple.next();					

	
							try {

								log.debug("Raw data : Append to hdfs");
								ObjectMapper mapper = new ObjectMapper();
								String jsonStr = mapper.readValue(tuple2._2,new TypeReference<String>(){});
								System.out.println(jsonStr);

	
							} catch (Exception e) {
								//check error and decide if to recycle msg if parser error.
								
								log.error(e,e);
								
							}

						}

					}

				});	

				return null;
			}
		});

		jssc.start();
		jssc.awaitTermination();

	}
}
