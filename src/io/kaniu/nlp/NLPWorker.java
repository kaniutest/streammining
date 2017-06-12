package io.kaniu.nlp;

import java.util.Properties;

import org.apache.log4j.Logger;

import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.parser.nndep.DependencyParser;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.PropertiesUtils;
import edu.stanford.nlp.util.logging.Redwood;


public class NLPWorker {
	
	static final Logger log = Logger.getLogger(NLPWorker.class);
	private static StanfordCoreNLP pipeline = null;
	
	 static{
		 
		 /*
		    Properties props = PropertiesUtils.asProperties(
		            "annotators", "tokenize,ssplit,pos,depparse",
		            "depparse.model", DependencyParser.DEFAULT_MODEL
		    );
		    */
		 
		    Properties props = PropertiesUtils.asProperties(
		            "annotators", "tokenize, ssplit, parse, sentiment"
		    );

		    pipeline = new StanfordCoreNLP(props);
	 }
	
	public static synchronized void process(String text){
		
		 Annotation ann = new Annotation(text);

		    pipeline.annotate(ann);

		    for (CoreMap sent : ann.get(CoreAnnotations.SentencesAnnotation.class)) {
		      SemanticGraph sg = sent.get(SemanticGraphCoreAnnotations.BasicDependenciesAnnotation.class);
		      log.info(IOUtils.eolChar + sg.toString(SemanticGraph.OutputFormat.LIST));
		    }
	}
	
	public static synchronized int sentiment(String text){

        
		int mainSentiment = 0;
        if (text != null && text.length() > 0) {
		 int longest = 0;
		 Annotation ann = pipeline.process(text);
		for (CoreMap sentence : ann.get(CoreAnnotations.SentencesAnnotation.class)) {
			Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            String partText = sentence.toString();
            if (partText.length() > longest) {
                mainSentiment = sentiment;
                longest = partText.length();
            }

        }
    }
    if (mainSentiment == 2 || mainSentiment > 4 || mainSentiment < 0) {
        return -1;
    }
    
      return mainSentiment;
	}

}
