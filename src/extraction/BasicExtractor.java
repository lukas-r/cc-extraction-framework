package extraction;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.MurmurHash3;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import com.google.common.collect.Lists;

import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import extraction.logging.Logger;
import extraction.output.DuplicateOutputObject;
import extraction.output.MatchOutputObject;
import extraction.output.OutputHandler;
import extraction.output.SentenceOutputObject;
import extraction.output.TermOutputObject;
import extraction.pattern.Matches;
import extraction.pattern.Pattern;
import extraction.pattern.StandardPatterns;
import utils.SentenceUtils;
import utils.Utils;
import utils.measures.EnumCounter;
import utils.measures.EnumTimer;
import utils.measures.MapCounter;
import utils.measures.MapTimer;

public class BasicExtractor extends Extractor {
	
	private final static Charset CHARSET = StandardCharsets.UTF_8;
	
	private final static int MAX_SENTENCE_LENGTH = 1000;
	private final static int MIN_SENTENCE_LENGTH = 10;
	
	enum CounterType {
		MATCHES, TUPLES, SENTENCES, MIN_SENTENCE_LENGTH_EXCLUSION, MAX_SENTENCE_LENGTH_EXCLUSION, PLD_EXTRACTION_ERROR, DUPLICATE_SENTENCE_EXCLUSION, OTHER_DOMAIN_SENTENCE_DUPLICATE, PRONOUN_FRONT_EXCLUSION, PRONOUN_BACK_EXCLUSION, TOTAL_RECORD_ERROR
	}

	enum TimerTarget {
		PROCESS, PRECHECKING_LINE, SENTENCE_SPLITTING, HASHING, PRECHECKING_SENTENCE, ANNOTATION, SENTIMENT_ANNOTATION, MATCHING, PLD_EXTRACTION, PRONOUN_CHECKING, WIKI_URL_EXTRACTION
	}

	@Override
	protected Map<String, String> extraction(ArchiveReader archive, OutputHandler output, Logger logger) {
		StanfordCoreNLP pipeline = getPipeline();
		//StanfordCoreNLP sentimentPipeline = getSentimentPipeline();
		Map<String, String> results = new LinkedHashMap<String, String>();
		
		EnumCounter<CounterType> counter = new EnumCounter<CounterType>(CounterType.class);
		EnumTimer<TimerTarget> timer = new EnumTimer<TimerTarget>(TimerTarget.class);
		MapCounter<String> patternCounter = new MapCounter<String>();
		MapTimer<String> patternTimer = new MapTimer<String>();

		Map<Long, Set<String>> hashedSentences = new HashMap<Long, Set<String>>();
		
		timer.start(TimerTarget.PROCESS);
		
		List<Pattern> patterns = StandardPatterns.LIST;
		java.util.regex.Pattern precheckPattern = Pattern.combinedPrecheckPattern(patterns);
		
		for (ArchiveRecord record: archive) {
			try {
				String url = record.getHeader().getUrl();
				BufferedReader br = new BufferedReader(new InputStreamReader(new BufferedInputStream(record), CHARSET));
				String line;
				String pld = null;
				timer.start(TimerTarget.PLD_EXTRACTION);
				try {
					pld = Utils.getPLD(record.getHeader().getUrl());
				} catch (IllegalStateException | IllegalArgumentException e) {
					counter.increase(CounterType.PLD_EXTRACTION_ERROR);
				}
				timer.stop(TimerTarget.PLD_EXTRACTION);
				while ((line = br.readLine()) != null) {
					if (line.length() < MIN_SENTENCE_LENGTH) {
						counter.increase(CounterType.MIN_SENTENCE_LENGTH_EXCLUSION);
						continue;
					}
					
					timer.start(TimerTarget.PRECHECKING_LINE);
					Matcher preCheckMatcher = precheckPattern.matcher(line);
					if (!preCheckMatcher.find()) {
						continue;
					}
					timer.stop(TimerTarget.PRECHECKING_LINE);
					
					timer.start(TimerTarget.SENTENCE_SPLITTING);
					ArrayList<String> sentences = SentenceUtils.filterSentences(SentenceUtils.splitLineToSentences(line, true), MIN_SENTENCE_LENGTH, MAX_SENTENCE_LENGTH, () -> counter.increase(CounterType.MAX_SENTENCE_LENGTH_EXCLUSION), () -> counter.increase(CounterType.MIN_SENTENCE_LENGTH_EXCLUSION));
					timer.stop(TimerTarget.SENTENCE_SPLITTING);
					
					counter.increase(CounterType.SENTENCES, sentences.size());
					for (String sentence: sentences) {
						timer.start(TimerTarget.HASHING);
						long hash = BasicExtractor.hash(sentence, false);
						timer.stop(TimerTarget.HASHING);
						Set<String> plds = hashedSentences.get(hash);
						if (plds != null) {
							if (plds.contains(pld)) {
								counter.increase(CounterType.DUPLICATE_SENTENCE_EXCLUSION);
							} else {
								counter.increase(CounterType.OTHER_DOMAIN_SENTENCE_DUPLICATE);
								plds.add(pld);
								output.output(new DuplicateOutputObject(sentence, pld, url));
							}
							continue;
						}
						
						CoreDocument document = new CoreDocument(sentence);
						timer.start(TimerTarget.ANNOTATION);
						pipeline.annotate(document);
						timer.stop(TimerTarget.ANNOTATION);
						boolean firstMatchPattern = true;
						
						for (Pattern pattern: patterns) {
							timer.start(TimerTarget.PRECHECKING_SENTENCE);
							if (pattern.precheckPattern.matcher(sentence).find()) {
								patternTimer.add(pattern.name, timer.stop(TimerTarget.PRECHECKING_SENTENCE));
								timer.start(TimerTarget.MATCHING);
								List<Matches> matches = pattern.getMatches(sentence, document, logger);
								timer.stop(TimerTarget.MATCHING);
								
								if (matches.size() > 0) {
									if (firstMatchPattern) {
										firstMatchPattern = false;
										/*timer.start(TimerTarget.SENTIMENT_ANNOTATION);
										sentimentPipeline.annotate(document);
										timer.stop(TimerTarget.SENTIMENT_ANNOTATION);
										String sentiment = document.sentences().get(0).sentiment();
										System.out.println(sentiment);*/
										output.output(new SentenceOutputObject(pattern, sentence, pld, url));
									}
									
									timer.start(TimerTarget.HASHING);
									Set<String> set = new HashSet<String>();
									set.add(pld);
									hashedSentences.put(hash(sentence, false), set);
									timer.stop(TimerTarget.HASHING);
									for (Matches match: matches) {
										patternCounter.increase(pattern.name, match.instances.size() * match.classes.size());
										counter.increase(CounterType.TUPLES, match.instances.size() * match.classes.size());
										counter.increase(CounterType.MATCHES);
										output.output(new MatchOutputObject(match.part, match.startPosition));
										for (TermOutputObject termOutput: match.getTermOutputObjects()) {
											output.output(termOutput);
										}
									}
								}
							}
						}
						
					}
				}
			} catch (IOException e) {
				logger.error("Archive IO Error", e);
			} catch (Exception e) {
				logger.error("Archive exception", e);
			}
		}
		timer.stop(TimerTarget.PROCESS);
		
		results.putAll(timer.getPrintableMap(null, "time", true));
		results.putAll(getPrintablePipelineInformation(pipeline, "annotation"));
		//results.putAll(getPrintablePipelineInformation(sentimentPipeline, "sentimentAnnotation"));
		results.putAll(counter.getPrintableMap(null, "count"));
		results.putAll(patternCounter.getPrintableMap("patternCount", null));
		results.putAll(patternTimer.getPrintableMap("patternTime", null, true));
		return results;
	}
	
	public static Map<String, String> getPrintablePipelineInformation(StanfordCoreNLP pipeline, String prefix) {
		return Lists.newArrayList(pipeline.timingInformation().split("\\r?\\n")).stream().map(l -> l.split(":")).filter(e -> e.length == 2).collect(Collectors.toMap(e -> prefix + "_" + e[0].trim(), e -> e[1].trim()));
	}
	
	private static long hash(String string, Boolean caseSensitive) {
		if (!caseSensitive) {
			string = string.toLowerCase();
		}
		int hash1 = MurmurHash3.hash32x86(string.getBytes(CHARSET));
		int hash2 = MurmurHash3.hash32x86(new StringBuilder(string).reverse().toString().getBytes(CHARSET));
		return (long) hash1 << 32 | hash2;
	}
	
	public static StanfordCoreNLP getPipeline() {
		Properties properties = new Properties();
		properties.put("annotators", "tokenize, ssplit, truecase, pos, lemma");
		properties.put("tokenize.options", "splitHyphenated=false");
		//properties.put("tokenize.asciiQuotes", true);
		properties.put("truecase.overwriteText", true);
		return new StanfordCoreNLP(properties);
	}
	
	public static StanfordCoreNLP getSentimentPipeline() {
		Properties properties = new Properties();
		properties.setProperty("enforceRequirements", "false");
		properties.put("annotators", "parse, sentiment");
		properties.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz");
		return new StanfordCoreNLP(properties);
	}

}
