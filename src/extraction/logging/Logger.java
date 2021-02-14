package extraction.logging;

public abstract class Logger {
	
	public enum Level {
		DEBUG, INFO, WARN, ERROR, FATAL
	}
	
	protected Level minLevel;
	
	public Logger() {
		this(null);
	}
	
	public Logger(Level minLevel) {
		this.minLevel = minLevel;
	}
	
	abstract public void log(Level level, String message, Exception exception);
	
	public void debug(String message, Exception exception) {
		this.log(Level.DEBUG, message, exception);
	}
	
	public void info(String message, Exception exception) {
		this.log(Level.INFO, message, exception);
	}
	
	public void warn(String message, Exception exception) {
		this.log(Level.WARN, message, exception);
	}
	
	public void error(String message, Exception exception) {
		this.log(Level.ERROR, message, exception);
	}
	
	public void fatal(String message, Exception exception) {
		this.log(Level.FATAL, message, exception);
	}
	
	public abstract void close();
}
