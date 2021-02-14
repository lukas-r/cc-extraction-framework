package extraction.logging;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ConsoleLogger extends Logger {
	
	public final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public void log(Level level, String message, Exception exception) {
		if (minLevel == null || level.ordinal() >= minLevel.ordinal()) {			
			System.out.println(df.format(new Date()) + "\t" + level.name() + "\t" + message);
			if (exception != null) {
				exception.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
	}

}
