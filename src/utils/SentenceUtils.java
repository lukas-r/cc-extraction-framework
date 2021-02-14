package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.webdatacommons.isadb.util.AbbreviationChecker;

import com.google.common.net.InternetDomainName;

import edu.stanford.nlp.process.WordToSentenceProcessor;

public abstract class SentenceUtils {
	
	private final static String domainPattern = "^(https?:\\/\\/)?([a-z]+\\.)+[a-z]+(?=\\s)";
	private final static Pattern pattern = Pattern.compile(domainPattern);
	
	public static ArrayList<String> splitLineToSentences(String line, boolean checkDomain) {
		line = line.replaceAll("\\s+", " ");
		ArrayList<String> sentences = new ArrayList<String>();
		
		final String sentenceEnding = WordToSentenceProcessor.DEFAULT_BOUNDARY_REGEX;
		final Pattern p = Pattern.compile(sentenceEnding);
		final Matcher m = p.matcher(line);
		
		int offset = 0;
		int pos = 0;
		boolean reset = true;
		while (reset ? m.find(pos) : m.find()) {
			reset = false;
			if (m.group().equals(".")) {
				int lastSpacePos = line.lastIndexOf(" ", m.start()) + 1;
				if (checkDomain) {
					final Matcher matcher = pattern.matcher(line.substring(lastSpacePos));
					if (matcher.find()) {
						if (matcher.group().toLowerCase().equals(matcher.group())) {
							String domain = Utils.getDomain(matcher.group());
							if (InternetDomainName.isValid(domain) && InternetDomainName.from(domain).isUnderRegistrySuffix()) {
								pos = matcher.end() + lastSpacePos;
								reset = true;
								continue;
							}
						}
					}
				}
				String previousWord = line.substring(lastSpacePos, m.start()).trim();
				if (previousWord.length() > 0 && AbbreviationChecker.getInstance().isAbbr(previousWord)) {
					continue;
				}
			}
			sentences.add(line.substring(offset, m.end()));
			offset = m.end();
		}
		if (offset < line.length()) {			
			sentences.add(line.substring(offset));
		}
		return sentences;
	}
	
	public static ArrayList<String> filterSentences(List<String> sentences, int minLength, int maxLength, Runnable maxCounterAdd, Runnable minCounterAdd) {
		ArrayList<String> filtered = new ArrayList<String>();
		for(String sentence: sentences) {
			sentence = sentence.trim();
			lengthLoop:
			while (sentence.length() > maxLength) {
				int pos = sentence.indexOf(" ");
				while(pos >= 0) {
					int newPos = sentence.indexOf(" ", pos + 1);
					if (newPos < 0 || newPos > maxLength) {
						String newSentence = sentence.substring(0, pos).trim();
						if (newSentence.length() < minLength) {
							minCounterAdd.run();
						} else if (newSentence.length() > maxLength) {
							maxCounterAdd.run();
						} else {
							filtered.add(newSentence);				
						}
						sentence = sentence.substring(pos + 1).trim();
						continue lengthLoop;
					}
					pos = newPos;
				}
				break;
			}
			if (sentence.length() < minLength) {
				minCounterAdd.run();
			} else if (sentence.length() > maxLength) {
				maxCounterAdd.run();
			} else {
				filtered.add(sentence);				
			}
		}
		return filtered;
	}
	
}
