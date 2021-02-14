package extraction.pattern;

import java.util.ArrayList;
import java.util.List;

import extraction.pattern.Pattern.Direction;

public class Match {
	
	public final TermType type;
	public final Term term;
	public final int position;
	public final int no;
	public final int level;
	public final boolean combined;
	
	public enum TermType {
		CLASS, INSTANCE
	}
	
	public Match(TermType type, Term term, int position, int no, int level, boolean combined) {
		this.type = type;
		this.term = term;
		this.position = position;
		this.no = no;
		this.level = level;
		this.combined = combined;
	}
	
	public static List<Match> fromSentenceParts(TermType type, List<SentencePart> parts, Direction direction) {
		List<Match> matches = new ArrayList<Match>();
		
		for (SentencePart part: parts) {
			try {
				if (direction == Direction.RIGHT_TO_LEFT) {
					matches.addAll(Term.getDeepMatches(type, part));
				} else {
					matches.add(new Match(type, new Term(part.part), part.position, part.no, 0, part.combined));
				}
			} catch (Exception e) {
			}
		}
		
		return matches;
	}

}
