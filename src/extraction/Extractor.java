package extraction;

import java.io.IOException;
import java.util.Map;

import org.archive.io.ArchiveReader;

import extraction.logging.Logger;
import extraction.output.OutputHandler;

public abstract class Extractor {
	
	public Map<String, String> extract(ArchiveReader archive, OutputHandler output, Logger logger) {
		Map<String, String> result = extraction(archive, output, logger);
		if (archive != null) {
			try {
				archive.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (output != null) {
			output.close();
		}
		return result;
	}
	
	protected abstract Map<String, String> extraction(ArchiveReader archive, OutputHandler output, Logger logger);

}
