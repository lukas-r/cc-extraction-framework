package tasks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import extraction.BasicExtractor;
import extraction.Extractor;
import extraction.logging.ConsoleLogger;
import extraction.logging.Logger;
import extraction.logging.TextLogger;
import extraction.output.OutputHandlerFactory;
import extraction.output.TextOutputHandler;
import queue.Queue;
import queue.TextQueue;
import runner.QueueExtractionRunner;
import runner.Runner;
import utils.CommonCrawlSource;
import utils.Utils;

public class RunBasicExtractor {
	
	private final static Options options;
	
	static {
		options = new Options();
		options.addOption("q", "queuePath", true, "Path to TextQeue directory.");
		options.addOption("o", "outputPath", true, "Path to save output.");
		options.addOption("t", "threads", true, "Number of threads to use.");
		options.addOption("z", "zip", false, "Whether to gzip output.");
		options.addOption("s", "stream", false, "Whether to download archives instead of streaming.");
		options.addOption("l", "log", true, "Path to logfile");
	}

	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
			
			String queuePath = cmd.getOptionValue("q");
			String outputPath = cmd.getOptionValue("o");
			int threadNo = Integer.parseInt(cmd.getOptionValue("t"));
			boolean zip = cmd.hasOption("z");
			boolean stream = cmd.hasOption("s");
			String logPath = cmd.getOptionValue("l");
			
			Extractor extractor = new BasicExtractor();
			Queue<String> queue = new TextQueue(new File(queuePath));
			
			OutputHandlerFactory factory = (String element, Logger logger) -> {
				File outputFile = new File(outputPath + File.separator + CommonCrawlSource.getFileNameFromUrl(element));
				try {
					Writer writer;
					if (zip) {
						writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputFile)));
					} else {
						writer = new FileWriter(outputFile);
					}
					return new TextOutputHandler(logger, new BufferedWriter(writer), false);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			};
			Logger logger = logPath != null ? new TextLogger(new BufferedWriter(new FileWriter(logPath, true))) : new ConsoleLogger();
			Runner runner = new QueueExtractionRunner(Utils.getHostname(), extractor, queue, factory, logger, threadNo, stream);
			runner.run();
			//logger.close();
		} catch (ParseException | IOException e) {
			e.printStackTrace();
		}
	}

}
