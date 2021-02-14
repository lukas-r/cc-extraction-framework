package test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import utils.SentenceUtils;

public class TestSentenceUtils {
	
	public static void testSentenceSplit() {
		String sentence = "Live bidding is also available via liveauctioneers.com	. Please note there is a 5% surcharge plus VAT for this service. Please use the link to register and refer to 'Buying' section of our website for terms.";
		//String sentence = "...perhaps, but ultimately, the choice is theirs, if they want to be held accountable and healed. I know we sometimes believe we want healing, yet subconsciously resist or don't feel ready, or worthy. Forgiving oneself is perhaps the first step, and the hardest. But those who believe they are above and beyond the need for forgiveness and redemption, that is a more challenging path, I suppose.";
		final String boundary = "(?<=\\s)(\\w+)(" + "\\." + ")(([a-zA-Z])\\w*)(?=\\s)";
		Pattern p = Pattern.compile(boundary);
		Matcher m = p.matcher(sentence);
		while (m.find()) {			
			for (int i = 0; i <= m.groupCount(); i++) {
				System.out.println(m.group(i));
			}
		}
		System.out.println(SentenceUtils.splitLineToSentences(sentence, true));
	}
	
	public static void main(String[] args) {
		testSentenceSplit();
	}

}
