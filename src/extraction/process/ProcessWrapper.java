package extraction.process;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.archive.io.ArchiveReader;
import org.archive.io.warc.WARCReaderFactory;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.processor.FileProcessor;
import org.webdatacommons.framework.processor.ProcessingNode;

import extraction.Extractor;
import extraction.logging.Log4jLogger;
import extraction.logging.Logger;
import extraction.output.TextOutputHandler;
import extraction.output.OutputHandler;

public abstract class ProcessWrapper extends ProcessingNode implements FileProcessor {
	private Extractor extractor;
	private org.apache.log4j.Logger logger;
	
	protected ProcessWrapper(Class<? extends Extractor> extractorClass) {
		this.logger = org.apache.log4j.Logger.getLogger(extractorClass);
		try {
			this.extractor = extractorClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	private static ArchiveReader readerFromByteChannel(ReadableByteChannel channel, String filename) throws IOException {
		InputStream inputStream = Channels.newInputStream(channel);
		return WARCReaderFactory.get(filename, inputStream, true);
	}

	@Override
	public Map<String, String> process(ReadableByteChannel fileChannel, String inputFileKey) throws Exception {
		Map<String, String> dataStats = new HashMap<String, String>();
		//TODO: bind datastats to extractor
		try {
			File tempOutputFile = File.createTempFile("cc-isadb-extraction", ".tar.gz");
			tempOutputFile.deleteOnExit();
			BufferedWriter bw = new BufferedWriter(
					new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(tempOutputFile))));
			
			ArchiveReader reader = ProcessWrapper.readerFromByteChannel(fileChannel, inputFileKey);
			Logger logger = new Log4jLogger(this.logger);
			OutputHandler output = new TextOutputHandler(logger, bw, true);
			this.extractor.extract(reader, output, logger);
			
			try {
				String outputFileKey = "data/ex_" + inputFileKey.replace("/", "_") + ".isadb.gz";
				S3Object dataFileObject;
				dataFileObject = new S3Object(tempOutputFile);
				dataFileObject.setKey(outputFileKey);
				getStorage().putObject(getOrCry("resultBucket"), dataFileObject);
			} catch (S3ServiceException | NoSuchAlgorithmException | IOException e) {
				this.logger.fatal("Error in S3 speicher Block", e);
			}
		} catch (FileNotFoundException ex) {
			this.logger.fatal(ex + " in " + inputFileKey + " while creating output file", ex);
		} catch (IOException ex) {
			this.logger.fatal(ex + " in " + inputFileKey + " while creating output file", ex);
		}
		return dataStats;
	}

}
