package extraction.pattern;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import extraction.pattern.Match.TermType;

public class Term {
	
	public final String nouns;
	public final String[] nounTags;
	public final String lemmas;
	public final String preMod;
	public final String[] preModTags;
	public final String preModSing;
	public final String postMod;
	public final String[] postModTags;
	
	private final static TokenSequencePattern pattern = TokenSequencePattern.compile("^" + PatternHelper.getPatternPhrase(true) + "$");
	
	public Term(String preMod, String preModSing, String[] preModTags, String nouns, String lemmas, String[] nounTags, String postMod, String[] postModTags) {
		this.preMod = preMod;
		this.preModSing = preModSing;
		this.preModTags = preModTags;
		this.nouns = nouns;
		this.lemmas = lemmas;
		this.nounTags = nounTags;
		this.postMod = postMod;
		this.postModTags = postModTags;
	}
	
	@SuppressWarnings("unchecked")
	public Term(List<CoreLabel> sentence) {
		TokenSequenceMatcher matcher = pattern.getMatcher(sentence);
		matcher.find();
		List<CoreLabel> preMod = (List<CoreLabel>) matcher.groupNodes(PatternHelper.GROUP_PREMODIFIER);
		String preModStr = matcher.group(PatternHelper.GROUP_PREMODIFIER);
		List<CoreLabel> postMod = (List<CoreLabel>) matcher.groupNodes(PatternHelper.GROUP_POSTMODIFIER);
		String postModStr = matcher.group(PatternHelper.GROUP_POSTMODIFIER);
		List<CoreLabel> nouns = (List<CoreLabel>) matcher.groupNodes(PatternHelper.GROUP_NOUNS);
		this.nouns = matcher.group(PatternHelper.GROUP_NOUNS);
		this.lemmas = String.join(" ", nouns.stream().map(l -> l.lemma()).collect(Collectors.toList()));
		this.nounTags = nouns.stream().map(l -> l.tag()).toArray(String[]::new);
		
		if (preModStr == null) {
			this.preMod = "";
		} else {
			this.preMod = preModStr;
		}
		if (this.preMod.equals("")) {
			this.preModTags = new String[]{};
			this.preModSing = "";
		} else {
			this.preModTags = preMod.stream().map(l -> l.tag()).toArray(String[]::new);	
			this.preModSing = String.join(" ", preMod.stream().map(l -> l.lemma()).collect(Collectors.toList()));
		}
		
		if (postModStr == null) {
			this.postMod = "";
		} else {
			this.postMod = postModStr;
		}
		if (this.postMod.equals("")) {
			this.postModTags = new String[]{};
		} else {
			this.postModTags = postMod.stream().map(l -> l.tag()).toArray(String[]::new);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static List<CoreLabel> getPostTerm(List<CoreLabel> sentence) {
		TokenSequenceMatcher matcher = pattern.getMatcher(sentence);
		matcher.find();
		return (List<CoreLabel>) matcher.groupNodes(PatternHelper.GROUP_POSTTERM);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Term other = (Term) obj;
		if (nouns == null) {
			if (other.nouns != null)
				return false;
		} else if (!nouns.equals(other.nouns))
			return false;
		if (postMod == null) {
			if (other.postMod != null)
				return false;
		} else if (!postMod.equals(other.postMod))
			return false;
		if (preMod == null) {
			if (other.preMod != null)
				return false;
		} else if (!preMod.equals(other.preMod))
			return false;
		return true;
	}
	
	public static List<Match> getDeepMatches(TermType type, SentencePart sentence) {
		List<Match> matches = new ArrayList<Match>();
		try {			
			matches.add(new Match(type, new Term(sentence.part), sentence.position, sentence.no, 0, sentence.combined));
			matches.addAll(getPostModMatches(type, sentence));
		} catch (Exception e) {
		}
		return matches;
	}
	
	public static List<Match> getPostModMatches(TermType type, SentencePart sentence) {
		List<Match> matches = new ArrayList<Match>();
		final int MAX_DEPTH = 2;
		List<CoreLabel> postMod = sentence.part;	
		try {
			for (int i = 0; i < MAX_DEPTH && postMod != null && postMod.size() > 0; i++) {
				int diff = postMod.size();
				postMod = Term.getPostTerm(postMod);
				diff -= postMod.size() - 1;
				matches.add(new Match(type, new Term(postMod), sentence.position - diff, sentence.no, i + 1, sentence.combined));
			}
		} catch (Exception e) {
		}
		return matches;
	}

}
