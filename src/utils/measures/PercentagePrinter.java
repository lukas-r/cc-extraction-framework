package utils.measures;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class PercentagePrinter {
	
	public final static DateTimeFormatter df = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
	
	private final String prefix;
	private final long max;
	private final int step;
	private final boolean multiLine;
	private final boolean printTime;
	
	private long count;

	public PercentagePrinter(long max, String prefix, int step, boolean multiLine, boolean printTime) {
		this.count = 0;
		this.prefix = prefix;
		this.max = max;
		this.step = step;
		this.multiLine = multiLine;
		this.printTime = printTime;
	}
	
	public void printAndCount() {
		if (this.count == 0 && this.multiLine) {
			System.out.println(df.format(LocalDateTime.now()));
		}
		if (step > 0) {
			if (this.count % this.step == 0) {
				this.print();
			}
		} else {
			if (Math.round(10000 * this.count / this.max) < Math.round(10000 * (this.count + 1) / this.max)) {
				this.print();
			}
		}
		this.count();
	}
	
	public void count() {
		this.count++;
	}
	
	private void print() {
		String print = String.format(prefix + " %.2f%%", 100.0D * this.count / this.max);
		if (this.printTime) {
			print += "\t" +  df.format(LocalDateTime.now());
		}
		print += "\t" + this.count;
		if (this.multiLine) {
			print = print + System.lineSeparator();
		} else {
			print = "\r" + print;
		}
		System.out.print(print);
	}
	
}
