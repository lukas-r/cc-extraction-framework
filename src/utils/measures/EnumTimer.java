package utils.measures;

public class EnumTimer<T extends Enum<T>> extends MapTimer<T> {
	
	public EnumTimer(Class<T> enumClass) {
	}
	
	@Override
	protected String getKeyName(T key) {
		return key.name();
	}
	
}
