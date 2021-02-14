package utils.measures;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.webdatacommons.isadb.util.Helper;

public class MapCounter<T> {
	
	Map<T, Integer> counts;
	
	public MapCounter() {
		this.counts = new LinkedHashMap<T, Integer>();
	}
	
	public void increase(T key, int amount) {
		this.counts.put(key, this.getCount(key) + amount);
	}
	
	public void increase(T key) {
		this.increase(key, 1);
	}
	
	public int getCount(T key) {
		return this.counts.getOrDefault(key, 0);
	}
	
	public Map<T, Integer> getCountMap() {
		return this.counts;
	}
	
	protected String getKeyName(T key) {
		return key.toString();
	}
	
	public Map<String, String> getPrintableMap(String prefix, String suffix) {
		if (prefix != null && prefix.length() > 0) {
			prefix = prefix + "_";
		} else {
			prefix = "";
		}
		if (suffix != null && suffix.length() > 0) {
			suffix = "_" + suffix;
		} else {
			suffix = "";
		}
		Map<String, String> printableMap = new LinkedHashMap<String, String>();
		for (Entry<T, Integer> entry: this.counts.entrySet()) {
			printableMap.put(prefix + Helper.toCamelCase(getKeyName(entry.getKey()) + suffix), entry.getValue().toString());
		}
		return printableMap;
	}
	
}
