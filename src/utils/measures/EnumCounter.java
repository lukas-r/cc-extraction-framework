package utils.measures;

public class EnumCounter<T extends Enum<T>> extends MapCounter<T> {
	
	public EnumCounter(Class<T> enumClass) {
		T[] enumConstants = enumClass.getEnumConstants();
		for (int i = 0; i < enumConstants.length; i++) {
			this.counts.put(enumConstants[i], 0);
		}
	}
	
	@Override
	protected String getKeyName(T key) {
		return key.name();
	}
	
}
