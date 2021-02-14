package utils.measures;

import java.util.ArrayList;

public class Timer {

	ArrayList<Long> startTimes;
	ArrayList<Long> totalTimes;
	
	public Timer(int size) {
		this.startTimes = new ArrayList<Long>(size);
		this.totalTimes = new ArrayList<Long>(size);
		for (int i = 0; i < size; i++) {
			this.startTimes.add(0L);
			this.totalTimes.add(0L);
		}
	}
	
	public void set(int index, long time) {
		this.totalTimes.set(index, time);
	}
	
	public void increase(int index, long time) {
		if (this.totalTimes.get(index) != null) {
			time += this.time(index);
		}
		this.set(index, time);
	}
	
	public void start(int index, boolean reset) {
		this.startTimes.set(index, System.nanoTime());
		if (reset) {
			this.totalTimes.set(index, null);
		}
	}
	
	public void start(int index) {
		start(index, false);
	}
	
	public long current(int index) {
		return System.nanoTime() - this.startTimes.get(index);
	}
	
	public void reset(int index) {
		this.totalTimes.set(index, null);
	}
	
	public long time(int index) {
		return this.totalTimes.get(index);
	}
	
	public String printable(int index, boolean inSec) {
		long time = this.totalTimes.get(index);
		if (inSec) {
			time /= 1000;
		}
		return Long.toString(time);
	}
	
	public void stop(int index, boolean add) {
		long time = System.nanoTime() - this.startTimes.get(index);
		if (add) {
			this.increase(index, time);
		} else {
			this.set(index, time);
		}
	}
	
	public void stop(int index) {
		stop(index, true);
	}
	
	public ArrayList<Long> getTimerList() {
		return this.totalTimes;
	}
	
}
