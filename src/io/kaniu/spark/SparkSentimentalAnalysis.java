package io.kaniu.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.elasticsearch.spark.streaming.EsSparkStreaming;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

import io.kaniu.nlp.NLPWorker;


public class SparkSentimentalAnalysis implements Serializable{
	
	static final Logger log = Logger.getLogger(SparkSentimentalAnalysis.class);
	
	static private Properties consumerProperties = new Properties();
	static private Properties esProperties = new Properties();
	static{
		
		InputStream producerprops = SparkTweetStreamer.class.getClassLoader().getResourceAsStream("kafkaconsumer.properties");
		InputStream esprops = SparkTweetStreamer.class.getClassLoader().getResourceAsStream("es-mappings.properties");
		try {
			consumerProperties.load(producerprops);
			esProperties.load(esprops);
		} catch (IOException e) {
			log.error(e);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static final long serialVersionUID = 1L;
	private static final String APP_NAME = "streamanalysis";
	
	private String kafkainputTopic = null;
	private int numThreads = 1;
	
	private SparkSentimentalAnalysis(){}
	
	public SparkSentimentalAnalysis(String _kafkainputTopic, int _numThreads){
		kafkainputTopic = _kafkainputTopic;
		numThreads = _numThreads;
	}
	
	public void run() throws IOException{

		SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).set("spark.driver.allowMultipleContexts","true");
		  
		if (!sparkConf.contains("spark.master")) {
		      sparkConf.setMaster("local[2]");
		 }
		
		//set ES properties to spark
		Set<Entry<Object, Object>> esEntries = esProperties.entrySet();
		for(Entry<Object, Object> en : esEntries){
			sparkConf.set(en.getKey().toString(), en.getValue().toString());
		}
		
		
		// Create the context with 2000 milliseconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

		String[] topics = kafkainputTopic.split(",");
  
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

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, byte[]>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, byte[]> tuple2) throws Exception {
				try {

					log.debug("Raw data : Append to hdfs");
					ObjectMapper mapper = new ObjectMapper();
					ObjectNode json = (ObjectNode) mapper.readTree(tuple2._2);
					
					json = filterAttributes(json);
					
					//String jsonStr = mapper.readValue(,new TypeReference<String>(){});
					String text =  json.get("text").textValue();
					//System.out.println("text ---> "+text);

					int sentiment = NLPWorker.sentiment(text);
					json.put("sentiment", sentiment);
					
					//System.out.println("data ---> "+json.toString());
					
					return json.toString();
					
				} catch (Exception e) {
					//check error and decide if to recycle msg if parser error.
					
					log.error(e,e);
					
				}
				return null;
			}

			private ObjectNode filterAttributes(ObjectNode _json) {
				ObjectNode json = JsonNodeFactory.instance.objectNode();
				json.put("text",_json.get("text"));
				
				ObjectNode location = JsonNodeFactory.instance.objectNode();
				location.put("lat", _json.get("geoLocation").get("latitude"));
				location.put("lon", _json.get("geoLocation").get("longitude"));
				
				json.put("location",location);
				
				ObjectNode place = JsonNodeFactory.instance.objectNode();
				place.put("geometryCoordinates",_json.get("place").get("geometryCoordinates"));
				place.put("geometryType",_json.get("place").get("geometryType"));
				
				json.put("place",place);
				json.put("createdAt",_json.get("createdAt"));
				return json;
			}});
		
		//lines.print();
		JavaEsSparkStreaming.saveJsonToEs(lines, "mining_index/tweet");
		//EsSparkStreaming.saveJsonToEs(lines.dstream(), "spark/docs");
		
		

		jssc.start();
		jssc.awaitTermination();

	}
	
	

}
