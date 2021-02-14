package utils.measures;

import java.util.ArrayList;

public class Counter {

	ArrayList<Integer> counts;
	
	public Counter(int size) {
		this.counts = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			this.counts.add(0);
		}
	}
	
	public void increase(int index, int amount) {
		this.counts.set(index, this.counts.get(index) + amount);
	}
	
	public void increase(int index) {
		increase(index, 1);
	}
	
	public int getCount(int index) {
		return this.counts.get(index);
	}
	
	public ArrayList<Integer> getCountList() {
		return this.counts;
	}
	
}
