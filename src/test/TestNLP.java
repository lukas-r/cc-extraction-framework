package test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreEntityMention;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import utils.SentenceUtils;

public class TestNLP {

	public static void testCoRef() throws Exception {
	    CoreDocument document = new CoreDocument("Barack Obama was born in Hawaii.  He is the president. Obama was elected in 2008.");
	    Properties props = new Properties();
	    
	    props.setProperty("pos.model", "english-left3words-distsim.tagger");
	    props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner,parse,coref");
	    
	    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
	    pipeline.annotate(document);
	    System.out.println("---");
	    System.out.println("coref chains");
	    for (CorefChain cc : document.corefChains().values()) {
	      System.out.println("\t" + cc);
	    }
	    for (CoreSentence sentence : document.sentences()) {
	      System.out.println("---");
	      System.out.println("mentions");
	      for (CoreEntityMention m : sentence.entityMentions()) {
	        System.out.println("\t" + m);
	       }
	      for (CoreLabel word: sentence.tokens()) {
	    	  if (word.word().equals("He") || word.word().equals("Barack Obama") || word.word().equals("Hawaii")) {
	    		  System.out.println("XXXXXXX");
	    	  }
	      }
	    }
	}
	
	public static void testPattern() {
		List<String> sentences = SentenceUtils.splitLineToSentences("Barack Obama was born in Hawaii. He is the president. Obama was elected in 2008.", false);
		System.out.println(sentences.size());
		for (String sentence : sentences) {
			Properties properties = new Properties();
			properties.setProperty("pos.model", "english-left3words-distsim.tagger");
			properties.put("annotators", "tokenize, ssplit, truecase, pos, lemma, ner, parse, natlog, openie");
			properties.put("truecase.overwriteText", true);
			StanfordCoreNLP pipeline = new StanfordCoreNLP(properties);
			
		    CoreDocument document = new CoreDocument(sentence);
		    pipeline.annotate(document);
		    
			List<CoreSentence> sentences_ = document.sentences();
			List<String> output_ = new ArrayList<>();
			String regex = "([{pos:/NN|NNS|NNP/}])"; //extracting Nouns
			
			for (CoreSentence sentence_ : sentences_) {
				List<CoreLabel> tokens = sentence_.tokens();
			    TokenSequencePattern tspattern = TokenSequencePattern.compile(regex);
			    TokenSequenceMatcher tsmatcher = tspattern.getMatcher(tokens);
			
			    while (tsmatcher.find()) {						
			        output_.add(tsmatcher.group());
			    }
			}
			System.out.println("Output: "+output_);
		}
	}
	
	public static void main(String[] args) throws Exception {
		testPattern();
	}
	
}
