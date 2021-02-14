package extraction.output;

import extraction.logging.Logger;

public abstract class OutputHandler {
	Logger logger;
	
	public OutputHandler(Logger logger) {
		this.logger = logger;
	}
	
	public abstract void output(OutputObject object);
	
	public abstract void close();

}
