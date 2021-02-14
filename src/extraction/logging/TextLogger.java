package extraction.logging;

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.exception.ExceptionUtils;

public class TextLogger extends Logger {
	
	public final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final Writer writer;
	
	public TextLogger(Writer writer) {
		this(writer, null);
	}
	
	public TextLogger(Writer writer, Level minLevel) {
		super(minLevel);
		this.writer = writer;
	}

	@Override
	public void log(Level level, String message, Exception exception) {
		if (minLevel == null || level.ordinal() >= minLevel.ordinal()) {			
			try {
				writer.write(df.format(new Date()) + "\t" + level.name() + "\t" + message);
				this.writer.append(System.lineSeparator());
				if (exception != null) {
					this.writer.append(exception.toString());
					this.writer.append(ExceptionUtils.getFullStackTrace(exception));
					this.writer.append(System.lineSeparator());
				}
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
		try {
			this.writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
