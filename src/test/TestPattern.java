package test;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import edu.stanford.nlp.ling.tokensregex.Env;
import edu.stanford.nlp.ling.tokensregex.NodePattern;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import extraction.BasicExtractor;
import extraction.logging.ConsoleLogger;
import extraction.output.MatchOutputObject;
import extraction.output.OutputHandler;
import extraction.output.TermOutputObject;
import extraction.output.TextOutputHandler;
import extraction.pattern.Match;
import extraction.pattern.Matches;
import extraction.pattern.Pattern;
import extraction.pattern.PatternHelper;
import extraction.pattern.StandardPatterns;
import extraction.pattern.Term;

public class TestPattern {
	
	public static void matchPattern(Pattern pattern, String sentence) {
		StanfordCoreNLP pipeline = BasicExtractor.getPipeline();
		CoreDocument document = new CoreDocument(sentence);
		pipeline.annotate(document);
		List<Matches> matchesList = pattern.getMatches(sentence, document, new ConsoleLogger());
		OutputHandler output = new TextOutputHandler(null, new PrintWriter(System.out), false);
		for (Matches matches: matchesList) {
			output.output(new MatchOutputObject(matches.part, matches.startPosition));
			System.out.println("________");
			for (Match match: matches.classes) {
				output.output(new TermOutputObject(match));
			}
			System.out.println("____");
			for (Match match: matches.instances) {
				output.output(new TermOutputObject(match));
			}
		}
	}
	
	private static void printPatternMatches(String matchStr, String sentence) {
		StanfordCoreNLP pipeline = BasicExtractor.getPipeline();
		CoreDocument document = new CoreDocument(sentence);
		pipeline.annotate(document);
		System.out.println(document.sentences().get(0).tokens());
		System.out.println(document.sentences().get(0).posTags());
		Env env = TokenSequencePattern.getNewEnv();
		env.setDefaultStringMatchFlags(NodePattern.CASE_INSENSITIVE | java.util.regex.Pattern.UNICODE_CASE);
	    env.setDefaultStringPatternFlags(java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.UNICODE_CASE);
		TokenSequencePattern matchPattern = TokenSequencePattern.compile(env, matchStr);
		TokenSequenceMatcher matcher = matchPattern.getMatcher(Pattern.filterQuotationMarks(document.sentences().get(0).tokens()));
		while (matcher.find()) {
			System.out.println(matcher.group(0));
			System.out.println(matcher.group(PatternHelper.GROUP_POSTTERM));
			System.out.println("__");
		}
	}
	
	public static void printTerm(Term term) {
		System.out.println(term.preMod);
		System.out.println(term.preModSing);
		System.out.println(Arrays.toString(term.preModTags));
		System.out.println(term.nouns);
		System.out.println(term.lemmas);
		System.out.println(Arrays.toString(term.nounTags));
		System.out.println(term.postMod);
		System.out.println(Arrays.toString(term.postModTags));
	}

	public static void main(String[] args) {
		Pattern pattern = StandardPatterns.PATTERN_SIC_ARE_THE;
		String sentence = "They have now discovered, and believe, that the storms with the highest and coldest clouds are the ones that can travel into the Atlantic, and once they meet the humid temperatures and high winds mid Atlantic, become tropical storms.";
		matchPattern(pattern, sentence);
	}
	
}
