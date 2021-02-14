package extraction.logging;

public class Log4jLogger extends Logger {
	org.apache.log4j.Logger logger;
	
	public Log4jLogger(org.apache.log4j.Logger logger) {
		this.logger = logger;
	}

	@Override
	public void log(Level level, String message, Exception exception) {
		switch(level) {
			case DEBUG:
				this.logger.debug(message, exception);
				break;
			case ERROR:
				this.logger.error(message, exception);
				break;
			case FATAL:
				this.logger.fatal(message, exception);
				break;
			case INFO:
				this.logger.info(message, exception);
				break;
			case WARN:
				this.logger.warn(message, exception);
				break;
			default:
				break;
		}
	}

	@Override
	public void close() {
	}

}
