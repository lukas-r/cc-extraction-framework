package queue;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SimpleTimeZone;

public class QueueElement<T> {
	public final String worker;
	public final T element;
	public final Date date;
	
	final static DateFormat dateFormat;
	
	static {
		dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		dateFormat.setTimeZone(new SimpleTimeZone(0, "GMT"));
	}

	public QueueElement(String worker, T element, Date date) {
		this.worker = worker;
		this.element = element;
		this.date = date;
	}
	
	@Override
	public String toString() {
		return worker + "\t" + element + "\t" + dateFormat.format(date);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		QueueElement<?> other = (QueueElement<?>) obj;
		if (this.element == null) {
			if (other.element != null)
				return false;
		} else if (!element.equals(other.element))
			return false;
		return true;
	}

	@SuppressWarnings("unchecked")
	public static <U> QueueElement<U> fromString(String string) {
		String[] parts = string.split("\t", -1);
		Date date = null;
		try {
			date = dateFormat.parse(parts[2]);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new QueueElement<U>(parts[0], (U) parts[1], date);
	}
	
}
