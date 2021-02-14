package runner;

import java.io.File;
import java.net.URL;
import java.util.Map;

import org.archive.io.ArchiveReader;
import org.archive.io.warc.WARCReaderFactory;

import extraction.Extractor;
import extraction.logging.Logger;
import extraction.output.OutputHandlerFactory;
import queue.Queue;
import utils.CommonCrawlSource;
import utils.Utils;

public class QueueExtractionRunner extends Runner {
	
	public QueueExtractionRunner(String name, Extractor extractor, Queue<String> queue, OutputHandlerFactory factory, Logger logger, int threadNo, boolean stream) {
		super(threadNo, (String id)->{
			logger.info("START RUNNER " + id, null);
		}, (String id)->{
			logger.info("THREAD START " + id, null);
			String next = "";
			File tmp = null;
			while ((next = queue.pop(name + "_" + id)) != null) {
				logger.info("EXTRACT START " + id + " " + next, null);
				try {
					ArchiveReader archive;
					if (stream) {
						archive = CommonCrawlSource.archiveFromURL(next);
					} else {
						tmp = File.createTempFile("extractor_" + CommonCrawlSource.getFileNameFromUrl(next) + "_", "");
						tmp.deleteOnExit();
						Utils.download(new URL(next), tmp);
						archive = WARCReaderFactory.get(tmp);
					}
					Map<String, String> info = extractor.extract(archive, factory.create(next, logger), logger);
					logger.info(Utils.mapFormat(info, true), null);
					queue.done(next, name + "_" + id);
					logger.info("EXTRACT DONE " + id + " " + next, null);
				} catch (Exception e) {
					logger.error("RUNNER ERROR " + id + ":", e);
					queue.back(next, false);
				} finally {
					if (!stream) {
						try {
							if (tmp != null && tmp.exists()) {								
								tmp.delete();
							}
						} catch (Exception e2) {
						}
					}
				}
			}
			logger.info("THREAD DONE " + id, null);
		}, (String id)->{
			logger.info("END RUNNER " + id, null);
		}, () -> {
			return queue.getTodo().length == 0;
		});
	}

}
