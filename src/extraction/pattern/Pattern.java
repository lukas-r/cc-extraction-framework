package extraction.pattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.tokensregex.Env;
import edu.stanford.nlp.ling.tokensregex.NodePattern;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import extraction.logging.Logger;
import extraction.pattern.Match.TermType;

public class Pattern {
	
	public final String name;
	public final PatternType type;
	public final PatternOrder order;
	public final Direction instanceDirection;
	public final Direction classDirection;
	
	public final String before;
	public final String between;
	public final String split;
	
	public final String precheckStr;
	public final String matchStr;
	
	public final java.util.regex.Pattern precheckPattern;
	public final TokenSequencePattern matchPattern;
	
	public final Numerus classNumerus;
	public final Numerus instanceNumerus;
	
	public final InstanceCount instanceCount;
	
	public enum PatternType {
		SPLIT, COMPACT
	}
	
	public enum PatternOrder {
		INSTANCE_CLASS, CLASS_INSTANCE
	}
	
	public enum Numerus {
		UNDEFINED, SINGULAR, PLURAL
	}
	
	public enum InstanceCount {
		SINGLE, MULTIPLE
	}
	
	public enum Direction {
		LEFT_TO_RIGHT, RIGHT_TO_LEFT
	}
	
	public Pattern(String name, PatternType type, PatternOrder order, String precheck, String pattern, Numerus classNumerus, Numerus instanceNumerus, InstanceCount instanceCount, String before, String between, String split) {
		this.name = name;
		this.type = type;
		this.order = order;
		switch (this.order) {
			case CLASS_INSTANCE:
				this.classDirection = Direction.RIGHT_TO_LEFT;
				this.instanceDirection = Direction.LEFT_TO_RIGHT;
				break;
			case INSTANCE_CLASS:
			default:
				this.classDirection = Direction.LEFT_TO_RIGHT;
				this.instanceDirection = Direction.RIGHT_TO_LEFT;
				break;
		}
		this.precheckStr = precheck;
		this.precheckPattern = java.util.regex.Pattern.compile(this.precheckStr, java.util.regex.Pattern.CASE_INSENSITIVE);
		this.matchStr = pattern;
		Env env = TokenSequencePattern.getNewEnv();
		env.setDefaultStringMatchFlags(NodePattern.CASE_INSENSITIVE | java.util.regex.Pattern.UNICODE_CASE);
	    env.setDefaultStringPatternFlags(java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.UNICODE_CASE);
		this.matchPattern = TokenSequencePattern.compile(env, this.matchStr);
		this.classNumerus = classNumerus;
		this.instanceNumerus = instanceNumerus;
		this.instanceCount = instanceCount;
		this.before = before;
		this.between = between;
		this.split = split;
	}
	
	public static Pattern compact(String name, String before, String between, Numerus classNumerus, Numerus instanceNumerus, InstanceCount instanceCount) {
		String precheck = " " + before + " .{1,50} " + between + " ";
		return new Pattern(name, PatternType.COMPACT, PatternOrder.CLASS_INSTANCE ,precheck, PatternHelper.compactPattern(before, between, instanceCount), classNumerus, instanceNumerus, instanceCount, before, between, null);
	}
	
	public static Pattern compact(String name, String before, String between, String precheck, Numerus classNumerus, Numerus instanceNumerus, InstanceCount instanceCount) {
		return new Pattern(name, PatternType.COMPACT, PatternOrder.CLASS_INSTANCE, preparePrecheck(precheck), PatternHelper.compactPattern(before, between, instanceCount), classNumerus, instanceNumerus, instanceCount, before, between, null);
	}
	
	public static Pattern splitInstanceClass(String name, String split, Numerus classNumerus, Numerus instanceNumerus, InstanceCount instanceCount) {
		return new Pattern(name, PatternType.SPLIT, PatternOrder.INSTANCE_CLASS, " " + split + " ", PatternHelper.instanceClassPattern(split, instanceCount), classNumerus, instanceNumerus, instanceCount, null, null, split);
	}
	
	public static Pattern splitInstanceClass(String name, String split, String precheck, Numerus classNumerus, Numerus instanceNumerus, InstanceCount instanceCount) {
		return new Pattern(name, PatternType.SPLIT, PatternOrder.INSTANCE_CLASS, preparePrecheck(precheck), PatternHelper.instanceClassPattern(split, instanceCount), classNumerus, instanceNumerus, instanceCount, null, null, split);
	}
	
	public static Pattern splitClassInstance(String name, String split, Numerus classNumerus, Numerus instanceNumerus, InstanceCount instanceCount) {
		return new Pattern(name, PatternType.SPLIT, PatternOrder.CLASS_INSTANCE, " " + split + " ", PatternHelper.classInstancePattern(split, instanceCount), classNumerus, instanceNumerus, instanceCount, null, null, split);
	}
	
	public static Pattern splitClassInstance(String name, String split, String precheck, Numerus classNumerus, Numerus instanceNumerus, InstanceCount instanceCount) {
		return new Pattern(name, PatternType.SPLIT, PatternOrder.CLASS_INSTANCE, preparePrecheck(precheck), PatternHelper.classInstancePattern(split, instanceCount), classNumerus, instanceNumerus, instanceCount, null, null, split);
	}
	
	private static String preparePrecheck(String precheck) {
		if (precheck.startsWith("-")) {
			precheck = precheck.substring(1);
		} else {
			precheck = " " + precheck;
		}
		if (precheck.endsWith("-")) {
			precheck = precheck.substring(0, precheck.length() - 1);
		} else {
			precheck = precheck + " ";
		};
		return precheck;
	}
	
	@SuppressWarnings("unchecked")
	public List<Matches> getMatches(String sentence, CoreDocument document, Logger logger) {
		List<Matches> matches = new ArrayList<Matches>();
		for (CoreSentence s: document.sentences()) {
			TokenSequenceMatcher matcher = this.matchPattern.getMatcher(Pattern.filterQuotationMarks(s.tokens()));
			while (matcher.find()) {
				try {
					List<CoreLabel> classTokens = (List<CoreLabel>) matcher.groupNodes(PatternHelper.GROUP_PATTERN_CLASS);
					int classPos = this.classDirection == Direction.LEFT_TO_RIGHT ? 0 : classTokens.size();
					SentencePart classPart = new SentencePart(classTokens, classPos, 0, false);
					List<SentencePart> instanceParts = splitTokenList((List<CoreLabel>) matcher.groupNodes(PatternHelper.GROUP_PATTERN_INSTANCE), this.instanceDirection);
					
					List<Match> classList = Match.fromSentenceParts(TermType.CLASS, Arrays.asList(classPart), this.classDirection);
					List<Match> instanceList = Match.fromSentenceParts(TermType.INSTANCE, instanceParts, this.instanceDirection);
					
					Matches newMatches = new Matches(instanceList, classList, matcher.group(0), matcher.start(), matcher.start(PatternHelper.GROUP_PATTERN_ID));
					if (!matches.contains(newMatches)) {
						matches.add(newMatches);
					}
				} catch (Exception e) {
					logger.error("Matching error", e);
				}
			}
		}
		return matches;
	}
	
	private static List<SentencePart> splitTokenList(List<CoreLabel> list, Direction direction) {
		List<SentencePart> result = new ArrayList<SentencePart>();
		List<Pair<Integer, CoreLabel>> combinationCandidates = new ArrayList<Pair<Integer, CoreLabel>>();
		int lastPos = 0;
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i).value().matches(PatternHelper.PATTERN_CONJ_REGEX)) {
				if (i != 0 && lastPos < i) {
					Term term = null;
					try {
						term = new Term(list.subList(lastPos, i));
					} catch (Exception e) {}
					if (term != null) {
						result.add(new SentencePart(list.subList(lastPos, i), lastPos, result.size(), false));
						if (list.get(i).value().matches("(and|&)")) {
							combinationCandidates.add(Pair.of(result.size() - 1, list.get(i)));
						}
						lastPos = i + 1;
					}
				} else {
					lastPos = i + 1;
				}
			}
		}
		if (lastPos != list.size()) {			
			result.add(new SentencePart(list.subList(lastPos, list.size()), lastPos, result.size(), false));
		}
		for (Pair<Integer, CoreLabel> candidate: combinationCandidates) {
			List<CoreLabel> newSubList = new ArrayList<CoreLabel>();
			newSubList.addAll(result.get(candidate.getLeft()).part);
			newSubList.add(candidate.getRight());
			newSubList.addAll(result.get(candidate.getLeft() + 1).part);
			result.add(candidate.getLeft() + 2, new SentencePart(newSubList, result.get(candidate.getLeft()).position, result.get(candidate.getLeft()).no, true));
		}
		if (direction == Direction.RIGHT_TO_LEFT) {
			int noParts = result.size() - combinationCandidates.size();
			result = result.stream().map(p -> {
				int no = noParts - 1 - p.no;
				if (p.combined) {
					no -= 1;
				}
				return new SentencePart(p.part, list.size() - 1 - p.position, no, p.combined);
			}).collect(Collectors.toList());
		}
		return result;
	}
	
	public static List<CoreLabel> filterQuotationMarks(List<CoreLabel> tokens) {
		return tokens.stream().filter(t -> !t.tag().equals("``") && !t.tag().equals("''")).collect(Collectors.toList());
	}
	
	public static java.util.regex.Pattern combinedPrecheckPattern(List<Pattern> patterns) {
		StringBuilder str = new StringBuilder();
		str.append("(");
		str.append(String.join("|", patterns.stream().map(p -> p.precheckStr).collect(Collectors.toList())));
		str.append(")");
		return java.util.regex.Pattern.compile(str.toString());
	}

}
