package utils.measures;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import utils.Utils;

public class MapTimer<T> {

	Map<T, Long> startTimes;
	Map<T, Long> totalTimes;
	
	public MapTimer() {
		this.startTimes = Collections.synchronizedMap(new LinkedHashMap<T, Long>());
		this.totalTimes = Collections.synchronizedMap(new LinkedHashMap<T, Long>());
	}
	
	public synchronized void start(T key, boolean reset) {
		this.startTimes.put(key, System.nanoTime());
		if (reset) {
			this.totalTimes.remove(key);
		}
	}
	
	public synchronized void start(T key) {
		start(key, false);
	}
	
	public synchronized long current(T key) {
		return System.nanoTime() - this.startTimes.get(key);
	}
	
	public void reset(T key) {
		this.totalTimes.remove(key);
	}
	
	public synchronized long time(T key) {
		return this.totalTimes.get(key);
	}
	
	public synchronized String printable(T key, boolean inSec) {
		long time = this.totalTimes.get(key);
		if (inSec) {
			time /= 1000;
		}
		return Long.toString(time);
	}
	
	public synchronized void add(T key, long diff) {
		this.totalTimes.put(key, this.totalTimes.getOrDefault(key, 0L) + diff);
	}
	
	public synchronized long stop(T key, boolean add) {
		long difference = System.nanoTime() - this.startTimes.get(key);
		long total = difference;
		if (add) {
			if (this.totalTimes.containsKey(key)) {
				total += this.totalTimes.get(key);
			}
		}
		this.totalTimes.put(key, total);
		return difference;
	}
	
	public synchronized long stop(T key) {
		return stop(key, true);
	}
	
	public synchronized Map<T, Long> getTimerMap() {
		return this.totalTimes;
	}
	
	protected synchronized String getKeyName(T key) {
		return key.toString();
	}
	
	public synchronized void incorporate(MapTimer<T> timer) {
		for (Entry<T, Long> entry: timer.totalTimes.entrySet()) {
			this.add(entry.getKey(), entry.getValue());
		}
	}
	
	public synchronized Map<String, String> getPrintableMap(String prefix, String suffix, Boolean inSeconds) {
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
		for (Entry<T, Long> entry: this.totalTimes.entrySet()) {
			Long time = entry.getValue();
			if (inSeconds) {
				time /= 1000;
			}
			printableMap.put(prefix + Utils.toCamelCase(getKeyName(entry.getKey()) + suffix), time.toString());
		}
		return printableMap;
	}
	
	@Override
	public String toString() {
		return Utils.mapFormat(this.getPrintableMap(null, null, false), true);
	}
	
}
