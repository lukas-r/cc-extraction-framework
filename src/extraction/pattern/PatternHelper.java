package extraction.pattern;

import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;

import extraction.pattern.Pattern.InstanceCount;

public abstract class PatternHelper {
	private final static String ATTR_WORD = "word";
	private final static String ATTR_TAG = "tag";
	
	public final static String GROUP_PREMODIFIER = "$pre";
	public final static String GROUP_ADJECTIVES = "$adjs";
	public final static String GROUP_NOUNS = "$nouns";
	public final static String GROUP_POSTMODIFIER = "$post";
	public final static String GROUP_POSTTERM = "$post_term";
	public final static String GROUP_MAIN = "$main";
	public final static String GROUP_PATTERN_REP = "$pattern_rep";
	public final static String GROUP_PATTERN_CLASS = "$class";
	public final static String GROUP_PATTERN_INSTANCE = "$instance";
	public final static String GROUP_PATTERN_ID = "$pattern";
	
	private final static int MAX_ADJECTIVE_COUNT = 3;
	private final static int MAX_NOUN_COUNT = 3;
	private final static int MAX_PRETERM_ADJECTIVE_COUNT = 2;
	private final static int MAX_PRETERM_NOUN_COUNT = 2;
	private final static int MAX_PHRASE_COUNT = 4;
	
	public static enum POSTag {
		CC, CD, DT, EX, FW, IN, JJ, JJR, JJS, LS, MD, NN, NNS, NNP, NNPS, PDT, POS, PRP, PRP$, RB, RBR, RBS, RP, SYM, TO, UH, VB, VBD, VBG, VBN, VBP, VBZ, WDT, WP, WP$, WRB
	}
	
	public final static String PATTERN_QUOTATION_MARK = "(/\u2018/|/\u2019/|/\u201A/|/\u201B/|/\u201C/|/\u201D/|/\u201E/|/\"/)";
	public final static POSTag[] TAGS_NOUN = {POSTag.NN, POSTag.NNS, POSTag.NNP, POSTag.NNPS, POSTag.CD};
	public final static String PATTERN_COMB = getNounString(false) + buildTerm(disjunct(fromWord("and"), fromWord("&"))) + getNounString(false);
	public final static String PATTERN_ADJ = buildTerm(disjunct(conjunct(fromTag(POSTag.RB), not(fromWord("not"))), fromTag(POSTag.RBR), fromTag(POSTag.RBS) ,fromTag(POSTag.JJ), fromTag(POSTag.JJR), fromTag(POSTag.JJS), fromTag(POSTag.VBN), fromTag(POSTag.VBG), fromTag(POSTag.VBD), fromTag(POSTag.CD)));
	public final static String PATTERN_CON_IN = buildTerm(disjunct(fromTag(POSTag.VBN), fromTag(POSTag.IN)))+ "? " + buildTerm(fromTag(POSTag.IN));
	public final static String PATTERN_ADD_WDT = buildTerm(fromTag(POSTag.WDT)) + "(" + buildTerm(fromTag(POSTag.VBP)) + buildTerm(fromTag(POSTag.VBN)) + "?|" + buildTerm(fromTag(POSTag.MD)) + buildTerm(fromTag(POSTag.VB)) + ") " + PATTERN_ADJ;
	public final static String PATTERN_ADD_VBG = buildTerm(fromTag(POSTag.IN)) +  buildTerm(fromTag(POSTag.VBG)) + buildTerm(fromTag(POSTag.JJ)) + "?";
	public final static String PATTERN_ADD_TERM = getTermString(true, MAX_NOUN_COUNT - 1, MAX_ADJECTIVE_COUNT - 1, MAX_PRETERM_ADJECTIVE_COUNT - 1, MAX_PRETERM_NOUN_COUNT - 1);
	public final static String PATTERN_ADD_VB = buildTerm(fromTag(POSTag.TO)) +  buildTerm(fromTag(POSTag.VB)) + "(" + buildTerm(fromTag(POSTag.IN)) + "|" + "(?" + GROUP_POSTTERM + " " + PATTERN_ADD_TERM + "))?";
	public final static String PATTERN_ADD = "(?" + GROUP_POSTMODIFIER + " (" + PATTERN_CON_IN + "?(?" + GROUP_POSTTERM + " " + PATTERN_ADD_TERM + "(" + PATTERN_CON_IN + PATTERN_ADD_TERM + ")?) |" + PATTERN_ADD_VB +")|" + PATTERN_ADD_WDT + "|" + PATTERN_ADD_VBG + ")";
	public final static String PATTERN_CONJ = "(/,/|and|or)";
	public final static String PATTERN_CONJ_REGEX = "(,|and|or)";
	
	private static String getNounString(boolean withVbgNoun) {
		POSTag[] nounTags = TAGS_NOUN;
		if (withVbgNoun) {
			nounTags = (POSTag[]) ArrayUtils.add(nounTags, POSTag.VBG);
		}
		return buildTerm(disjunct(Arrays.stream(nounTags).map(PatternHelper::fromTag).toArray(String[]::new)));
	}
	
	public static String getNounGroupString(boolean withGroupName, boolean withVbgNoun, int maxCount) {
		return "(" + (withGroupName ? "?" + GROUP_NOUNS + " " : "") + getNounString(withVbgNoun) + "{1, " + maxCount + "}|" + PATTERN_COMB + ")";
	}
	
	private static String getAdjectiveGroupString(int maxCount) {
		return "((" + PATTERN_ADJ + " /,/?){0," + (maxCount - 1) + "}"+ PATTERN_ADJ + ")";
	}
	
	public static String getPreTermString(int maxAdjectiveCount, int maxNounCount) {
		return "(" + buildTerm(conjunct(fromTag(POSTag.DT), not(fromWord("no")))) + "|" + buildTerm(conjunct(fromTag(POSTag.DT), not(fromWord("no")))) + "? " + getAdjectiveGroupString(maxAdjectiveCount) + "? " + getNounGroupString(false, false, maxNounCount) + buildTerm(fromTag(POSTag.POS)) + ")";
	}
	
	private static String getTermString(boolean withVbgNoun, int maxNounCount, int maxAdjectiveCount, int maxPreTermAdjectiveCount, int maxPreTermNounCount) {
		return "(?" + GROUP_MAIN + " (?" + GROUP_PREMODIFIER + " " + getPreTermString(maxPreTermAdjectiveCount, maxNounCount) + "? (?" + GROUP_ADJECTIVES + " " + getAdjectiveGroupString(maxAdjectiveCount) + "?))" + getNounGroupString(true, withVbgNoun, maxNounCount) + ")";
	}
	
	private static String getPatternPhraseTerm(boolean withVbgNoun) {
		return getTermString(withVbgNoun, MAX_NOUN_COUNT, MAX_ADJECTIVE_COUNT, MAX_PRETERM_ADJECTIVE_COUNT, MAX_PRETERM_NOUN_COUNT);
	}
	
	public static String getPatternPhrase(boolean withVbgNoun) {
		return getPatternPhraseTerm(withVbgNoun) + PATTERN_ADD + "?";
	}
	
	public static String getPatternPhraseRep(boolean withVbgNoun) {
		return "(?" + GROUP_PATTERN_REP + getPatternPhrase(withVbgNoun) + " (" + PATTERN_CONJ + getPatternPhrase(withVbgNoun) + "){0," + (MAX_PHRASE_COUNT - 2) + "}(/,/?" + PATTERN_CONJ + " " + getPatternPhrase(withVbgNoun) + ")?)";
	}
	
	public static String buildTerm(String content) {
		return "[" + content + "]";
	}
	
	private static String buildBracket(String attribute, String content) {
		return "{" + attribute + ":" + content + "}";
	}
	
	public static String fromWord(String word) {
		return buildBracket(ATTR_WORD, "\"" + word + "\"");
	}
	
	public static String fromTag(POSTag tag) {
		return buildBracket(ATTR_TAG, tag.name());
	}
	
	public static String disjunct(String... brackets) {
		return String.join("|", brackets);
	}
	
	public static String conjunct(String... brackets) {
		return String.join("&", brackets);
	}
	
	public static String not(String input) {
		return "!" + input;
	}
	
	private static String getInstanceString(InstanceCount instanceCount, boolean withVbgNoun) {
		if (instanceCount == InstanceCount.SINGLE) {
			return getPatternPhrase(withVbgNoun);
		} else {
			return getPatternPhraseRep(withVbgNoun);
		}
	}
	
	public static String classInstancePattern(String identifier, InstanceCount instanceCount) {
		return "(?" + GROUP_PATTERN_CLASS + " " + getPatternPhrase(false) + ") /,/?(?" + GROUP_PATTERN_ID + " " + identifier + " )(?" + GROUP_PATTERN_INSTANCE + " " + getInstanceString(instanceCount, true) + ")";
	}
	
	public static String instanceClassPattern(String identifier, InstanceCount instanceCount) {
		return "(?" + GROUP_PATTERN_INSTANCE + " " + getInstanceString(instanceCount, true) + ") /,/?(?" + GROUP_PATTERN_ID + " " + identifier + ")(?" + GROUP_PATTERN_CLASS + " " + getPatternPhrase(false) + ")";
	}
	
	public static String compactPattern(String before, String between, InstanceCount instanceCount) {
		return  "(?" + GROUP_PATTERN_ID + " " + before + ")(?" + GROUP_PATTERN_CLASS + " " + getPatternPhrase(false) + ") /,/? " + between + " (?" + GROUP_PATTERN_INSTANCE + " " + getInstanceString(instanceCount, true) + ")";
	}

}
