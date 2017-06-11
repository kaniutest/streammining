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
		
		switch(APP_NAME){
		case("TWEETSTREAM"):
			if (args.length < 3) {
				System.err.println("Usage: "+APP_NAME+" <topics> <numThreads> <outputTopic>");
				System.exit(1);
			}
			new SparkTweetStreamer( args[1], 1, "tweet-topic" ).run();
			return;
		case("SENTIMENT_PROC"): 
			new SparkSentimentalAnalysis(  "tweet-topic",1 ).run();
			return;
		default: {
			System.err.println("application name not recognized! Expected: "
				+ "TWEETSTREAM, SENTIMENT_PROC");
			System.exit(1);
			return;
		}
		}//switch
			
		

		
	}

}
