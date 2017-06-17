package io.kaniu.spark;

import java.io.IOException;

/**
 * The main class for all spark jobs. 
 * */
public class MainAllJobRunner {
	
	
	public static void main(String[] args) throws IOException {
		

		if(args.length < 1){
			System.err.println("Specify an application name in arguments: Expected any of these: "
					+ "TWEETSTREAM, SENTIMENT_PROC");
			System.exit(1);
		}
		
		String APP_NAME = args[0];
		
		if (args.length < 3) {
			System.err.println("Usage: "+APP_NAME+" <topics> <numThreads>");
			System.exit(1);
		}
		
		String topic = null;
		Integer threads = null;
		try{
			topic = args[1];
			threads = Integer.parseInt(args[2]);
		}catch (Throwable e){
			
			System.err.println(e.getMessage()+"       Usage: "+APP_NAME+" <topics> <numThreads>");
			System.exit(1);
		}
		
		switch(APP_NAME){
		case("TWEETSTREAM"):

			new SparkTweetStreamer( topic, threads ).run();
			return;
		case("SENTIMENT_PROC"): 
			
			new SparkSentimentalAnalysis(  topic, threads  ).run();
			return;
		default: {
			System.err.println("application name '"+APP_NAME+"' not recognized! Expected: "
				+ "TWEETSTREAM, SENTIMENT_PROC");
			System.exit(1);
			return;
		}
		}//switch
			
		

		
	}

}
