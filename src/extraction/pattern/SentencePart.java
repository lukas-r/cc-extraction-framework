package extraction.pattern;

import java.util.List;

import edu.stanford.nlp.ling.CoreLabel;

public class SentencePart {
	
	public final List<CoreLabel> part;
	public final int position;
	public final int no;
	public final boolean combined;
	
	public SentencePart(List<CoreLabel> part, int position, int no, boolean combined) {
		this.part = part;
		this.position = position;
		this.no = no;
		this.combined = combined;
	}
	
	@Override
	public String toString() {
		return part + " pos: " + position + " no: " + no + " cmb: " + combined;
	}
	
}