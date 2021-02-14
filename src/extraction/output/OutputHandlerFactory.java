package extraction.output;

import extraction.logging.Logger;

public interface OutputHandlerFactory {
	
	public OutputHandler create(String element, Logger logger);

}
