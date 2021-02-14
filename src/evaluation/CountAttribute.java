package evaluation;

public enum CountAttribute {
	FREQUENCY("count"), PLD_SPREAD("pld_count");
	
	public final String column;
	
	CountAttribute(String column) {
		this.column = column;
	}
}