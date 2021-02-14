package extraction;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCRecord;
import org.webdatacommons.isadb.processor.EnhancedFastWetProcessor;
import org.webdatacommons.isadb.processor.EnhancedFastWetProcessor.CounterType;
import org.webdatacommons.isadb.processor.EnhancedFastWetProcessor.TimerTarget;
import org.webdatacommons.isadb.processor.FastWetProcessor;
import org.webdatacommons.isadb.tools.WarcConnection;
import org.webdatacommons.isadb.tools.WarcConnection.LinkFetcher;
import org.webdatacommons.isadb.util.Helper;
import org.webdatacommons.isadb.util.Helper.PatternMeasures;
import org.webdatacommons.isadb.util.NounPhrase;
import org.webdatacommons.isadb.util.measures.EnumCounter;
import org.webdatacommons.isadb.util.measures.EnumTimer;

import com.google.common.net.InternetDomainName;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import extraction.logging.Logger;
import extraction.output.OutputHandler;
import extraction.output.SimpleOutputObjectFactory;

public class EFWExtractor extends Extractor {

	@Override
	protected Map<String, String> extraction(ArchiveReader archive, OutputHandler output, Logger logger) {
		String inputFileKey = archive.getFileName();
		Map<String, String> dataStats = new LinkedHashMap<String, String>();
		SimpleOutputObjectFactory factory = new SimpleOutputObjectFactory(EnhancedFastWetProcessor.ATTRIBUTES);
		
		EnumCounter<CounterType> counter = new EnumCounter<CounterType>(CounterType.class);
		EnumTimer<TimerTarget> timer = new EnumTimer<TimerTarget>(TimerTarget.class);

		timer.start(TimerTarget.PROCESS);

		MaxentTagger tagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger");

		// Variables and Constants for false-positive detection
		String[] allExclusions = Helper.concatAll(Helper.DEMONSTRATIVES, Helper.POSSESSIVES, Helper.PERSONALS,
				Helper.QUESTIONS, Helper.OTHERS);

		// Initialize data lists
		PatternMeasures patternMeasures = new PatternMeasures(EnhancedFastWetProcessor.ALL_PATTERNS.size());

		// Variables for duplicate sentences
		ArrayList<HashSet<Integer>> allSentenceUrlHashes = new ArrayList<HashSet<Integer>>();
		for (int i = 0; i <= EnhancedFastWetProcessor.ALL_PATTERNS.size(); i++) {
			allSentenceUrlHashes.add(new HashSet<Integer>());
		}

		ArchiveRecord firstRecord = archive.iterator().next();
		String wetName = (String) firstRecord.getHeader()
				.getHeaderValue(WARCRecord.HEADER_KEY_FILENAME);

		timer.start(TimerTarget.WIKI_URL_EXTRACTION);
		LinkFetcher fetcher = null;
		try {
			fetcher = LinkFetcher.find(WarcConnection.getCrawl(firstRecord), WarcConnection.getWarcName(wetName));
		} catch (Exception e) {
			logger.error("LinkFetcher error", e);
		}
		timer.stop(TimerTarget.WIKI_URL_EXTRACTION);

		// iterate over each record in the stream
		for (ArchiveRecord record : archive) {
			try {
				String pld = null; // Private level domain of the record
				Map<String, Set<String>> urls = null;

				BufferedReader br = new BufferedReader(new InputStreamReader(new BufferedInputStream(record)));
				while (br.ready()) {
					String line = br.readLine();

					// If line is below min. Sentence-Length Processing is skipped
					if (line.length() < EnhancedFastWetProcessor.MIN_SENTENCE_LENGTH) {
						counter.increase(CounterType.MIN_SENTENCE_LENGTH_EXCLUSION);
						continue;
					}

					// Pre-check the line chunk for Pattern Keywords
					timer.start(TimerTarget.PRECHECKING);
					Matcher preCheckMatcher = EnhancedFastWetProcessor.PRECONDITION_PATTERN.matcher(line);
					if (!preCheckMatcher.find()) {
						continue;
					}
					timer.stop(TimerTarget.PRECHECKING);

					// Line is split into sentences
					timer.start(TimerTarget.SENTENCE_SPLITTING);
					ArrayList<String> sentences = Helper.filterSentences(Helper.splitLineToSentences(line),
							EnhancedFastWetProcessor.MIN_SENTENCE_LENGTH, EnhancedFastWetProcessor.MAX_SENTENCE_LENGTH);
					timer.stop(TimerTarget.SENTENCE_SPLITTING);

					counter.increase(CounterType.SENTENCES, sentences.size());
					for (String sentence : sentences) {
						// Help the Pos-Tagger with Apostrophies and QuotationMarks; Replace as many as
						// possible, without losing the meaning;
						sentence = sentence.replaceAll("\\s+", " ");
						sentence = Helper.replaceVerbApostrophies(sentence);
						sentence = sentence.replaceAll("(?<!s)" + Helper.QUOTATION_MARKS + "(?!s)", "");

						for (int i = 0; i < EnhancedFastWetProcessor.ALL_PATTERNS.size(); i++) {
							timer.start(TimerTarget.MATCHING);
							Matcher patternMatcher = EnhancedFastWetProcessor.ALL_PATTERNS.get(i).pattern.matcher(sentence);
							Boolean firstMatch = true;
							while (patternMatcher.find()) {
								//
								// Start Extract PLD
								//
								if (pld == null) {
									timer.start(TimerTarget.PLD_EXTRACTION);
									try {
										pld = Helper.getDomainFromUrl(record.getHeader().getUrl());

										// If an exception occurrs (e.g. URL is an IP-Adress or a public
										// suffix), leave tmpUrl as it is.
										pld = InternetDomainName.from(pld).topPrivateDomain().toString();
									} catch (IllegalStateException | IllegalArgumentException e) {
										//logger.info(e + " in " + inputFileKey + " for record "
										//		+ record.getHeader().getUrl(), e);
										counter.increase(CounterType.PLD_EXTRACTION_ERROR);
									}
									timer.stop(TimerTarget.PLD_EXTRACTION);
								}
								//
								// End Extract PLD
								//

								// Check if the sentence has already been processed/stored
								if (firstMatch
										&& allSentenceUrlHashes.get(i).contains((sentence + pld).hashCode())) {
									counter.increase(CounterType.DUPLICATE_SENTENCE_EXCLUSION);
									timer.stop(TimerTarget.MATCHING);
									timer.start(TimerTarget.MATCHING);
									break;
								}
								firstMatch = false;

								//
								// Start storing pattern and statistics
								//
								//String extractedPattern = patternMatcher.group();
								int onset = patternMatcher.start();
								int offset = patternMatcher.end();

								// Find leading pronoun and check if it can be excluded
								timer.start(TimerTarget.PRONOUN_CHECKING);
								boolean pronounExcluded = false;
								String pronounHolder = sentence.substring(0, onset);
								if (pronounHolder.indexOf(" ") != -1) {
									pronounHolder = pronounHolder.substring(pronounHolder.lastIndexOf(" ") + 1);
								}
								for (int f = 0; f < allExclusions.length && !pronounHolder.equals(""); f++) {
									if (pronounHolder.toLowerCase().equals(allExclusions[f])) {
										counter.increase(CounterType.PRONOUN_FRONT_EXCLUSION);
										pronounExcluded = true;
										break;
									}
								}

								// Find and check if a following pronoun can be excluded
								pronounHolder = sentence.substring(offset);
								if (pronounHolder.indexOf(" ") != -1) {
									pronounHolder = pronounHolder.substring(0, pronounHolder.indexOf(" "));
								}
								for (int f = 0; f < allExclusions.length && !pronounHolder.equals(""); f++) {
									if (pronounHolder.toLowerCase().equals(allExclusions[f])) {
										counter.increase(CounterType.PRONOUN_BACK_EXCLUSION);
										pronounExcluded = true;
										break;
									}
								}
								timer.stop(TimerTarget.PRONOUN_CHECKING);
								if (pronounExcluded) {
									timer.stop(TimerTarget.MATCHING);
									timer.start(TimerTarget.MATCHING);
									continue;
								}

								//
								// Start Extracting single Tupels
								//
								List<HasWord> fullWordList = SentenceUtils.toWordList(sentence.split(" "));
								List<TaggedWord> fullTaggedList = tagger.tagSentence(fullWordList);

								for (TaggedWord tw : fullTaggedList) {
									String potentialGenitiveWord = tw.word();
									if (potentialGenitiveWord.length() > potentialGenitiveWord
											.replaceAll("(?<=s)" + Helper.APOSTROPHES, "").length()
											|| potentialGenitiveWord.length() > potentialGenitiveWord
													.replaceAll(Helper.APOSTROPHES + "(?=s)", "").length()) {
										tw.setTag("JJ");
									}
								}

								List<TaggedWord> taggedWordsBeforePattern, taggedWordsAfterPattern;
								if (EnhancedFastWetProcessor.ALL_PATTERNS.get(i).type.equals("compact")) {
									taggedWordsBeforePattern = FastWetProcessor.getWordListSubset(0, onset,
											fullTaggedList);
									taggedWordsAfterPattern = FastWetProcessor.getWordListSubset(offset,
											sentence.length(), fullTaggedList);
								} else {
									taggedWordsBeforePattern = FastWetProcessor.getWordlistBeforeSplittedPattern(
											EnhancedFastWetProcessor.ALL_PATTERNS.get(i), sentence, onset, fullTaggedList);
									taggedWordsAfterPattern = FastWetProcessor.getWordlistAfterSplittedPattern(
											EnhancedFastWetProcessor.ALL_PATTERNS.get(i), sentence, onset, offset, fullTaggedList);
								}

								ArrayList<NounPhrase> currentNPsAfterPattern = new ArrayList<NounPhrase>();
								ArrayList<NounPhrase> currentNPsBeforePattern = new ArrayList<NounPhrase>();
								Collections.reverse(taggedWordsBeforePattern);
								FastWetProcessor.findNextNounPhraseReverse(0, taggedWordsBeforePattern,
										currentNPsBeforePattern);
								FastWetProcessor.findNextNounPhrase(0, taggedWordsAfterPattern,
										currentNPsAfterPattern);

								if (currentNPsAfterPattern.size() == 0 || currentNPsBeforePattern.size() == 0) {
									timer.stop(TimerTarget.MATCHING);
									timer.start(TimerTarget.MATCHING);
									continue;
								}
								//
								// End Extracting single Tuples
								//

								ArrayList<NounPhrase> instancePatterns, classPatterns;
								if (EnhancedFastWetProcessor.ALL_PATTERNS.get(i).instanceFirst) {
									instancePatterns = currentNPsBeforePattern;
									classPatterns = currentNPsAfterPattern;
								} else {
									instancePatterns = currentNPsAfterPattern;
									classPatterns = currentNPsBeforePattern;
								}
								
								long matchingTime = timer.stop(TimerTarget.MATCHING);

								timer.start(TimerTarget.WIKI_URL_EXTRACTION);
								if (urls == null && fetcher != null) {
									urls = fetcher.getLinks((String) record.getHeader()
											.getHeaderValue(WARCRecord.HEADER_KEY_REFERS_TO), true);
								}
								String[] wikiUrls = EnhancedFastWetProcessor.findWikiUrls(urls, instancePatterns, classPatterns);
								timer.stop(TimerTarget.WIKI_URL_EXTRACTION);

								output.output(factory.createObject(EnhancedFastWetProcessor.ALL_PATTERNS.get(i).pid,
										sentence, 
										FastWetProcessor.nounPhraseListToString(instancePatterns),
										FastWetProcessor.nounPhraseListTagsToString(instancePatterns),
										FastWetProcessor.nounPhraseListToString(classPatterns),
										FastWetProcessor.nounPhraseListTagsToString(classPatterns),
										Integer.toString(onset),
										Integer.toString(offset),
										pld,
										Long.toString(matchingTime / 1000),
										EnhancedFastWetProcessor.printableNounPhraseDistances(instancePatterns),
										EnhancedFastWetProcessor.printableNounPhraseDistances(classPatterns),
										wikiUrls[0],
										wikiUrls[1])
								);
								
								patternMeasures.increaseTimer(i, matchingTime);
								patternMeasures.increaseCounter(i);
								counter.increase(CounterType.TUPLES,
										currentNPsBeforePattern.size() * currentNPsAfterPattern.size());
								counter.increase(CounterType.MATCHES);
								allSentenceUrlHashes.get(i).add((sentence + pld).hashCode());

								//
								// End storing pattern and statistics
								//
								timer.start(TimerTarget.MATCHING);
							}
							timer.stop(TimerTarget.MATCHING);
						}
					}
				}
			} catch (Exception ex) {
				logger.error(ex + " in " + inputFileKey + " for record " + record.getHeader().getUrl(), ex);
				counter.increase(CounterType.TOTAL_RECORD_ERROR);
			}
		}
		if (fetcher != null) {
			fetcher.close();
		}

		// runtime and rate calculation
		timer.stop(TimerTarget.PROCESS);

		// create data file statistics and return
		dataStats.putAll(patternMeasures.printableMap(EnhancedFastWetProcessor.ALL_PATTERNS));
		dataStats.putAll(timer.getPrintableMap("time", true));
		dataStats.putAll(counter.getPrintableMap("count"));
		
		return dataStats;
	}

}
