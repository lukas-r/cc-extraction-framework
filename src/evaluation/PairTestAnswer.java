package evaluation;

import java.util.Arrays;
import java.util.LinkedHashMap;

public class PairTestAnswer {
	
	public final static LinkedHashMap<Integer, PairTestAnswer> ANSWERS;
	
	public enum Truth {
		CORRECT(1), UNCERTAIN(0), INCORRECT(-1);
		
		public final int level;
		
		Truth(int level) {
			this.level = level;
			
		}
		
		public boolean isTrue() {
			return this.level > 0;
		}
	}
	
	public final int no;
	public final String text;
	public final Truth truth;
	
	static {
		ANSWERS = new LinkedHashMap<Integer, PairTestAnswer>();
		Arrays.asList(
			new PairTestAnswer(1, "correct", Truth.CORRECT),
			new PairTestAnswer(2, "uncertain", Truth.UNCERTAIN),
			new PairTestAnswer(3, "incorrect", Truth.INCORRECT)
		).stream().forEach(a -> ANSWERS.put(a.no, a));
	}
	
	public PairTestAnswer(int no, String text, Truth truth) {
		this.no = no;
		this.text = text;
		this.truth = truth;
	}

}
