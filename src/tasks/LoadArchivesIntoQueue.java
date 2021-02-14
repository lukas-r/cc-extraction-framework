package tasks;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import queue.Queue;
import queue.TextQueue;
import utils.CommonCrawlSource;
import utils.CommonCrawlSource.WARC_TYPE;

public class LoadArchivesIntoQueue {
	
	private final static Options options;
	
	static {
		options = new Options();
		options.addOption("c", "crawl", true, "Name of the crawl to use.");
		options.addOption("q", "queuePath", true, "Path to TextQeue directory.");
		options.addOption("r", "range", true, "Define subrange of urls starting from 1 (e.g. 1-100).");
		options.addOption("p", "part", true, "Define fraction of urls to use starting from 1 (e.g. 1/20).");
		options.addOption("d", "delete", false, "Clear queue before loading");
	}
	
	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
			
			String crawl = cmd.getOptionValue("c");
			String queuePath = cmd.getOptionValue("q");
			String range = cmd.getOptionValue("r");
			String part = cmd.getOptionValue("p");
			boolean reset = cmd.hasOption("d");
			
			List<String> archives = new CommonCrawlSource(crawl).archiveList(WARC_TYPE.WET, true);
			Queue<String> queue = new TextQueue(new File(queuePath));
			
			if (reset) {
				queue.reset();
			}
			
			int start = 0;
			int end = archives.size();
			
			if (range != null) {
				String[] parts = range.split("-");
				start = Math.max(Integer.parseInt(parts[0]) - 1, 0);
				end = Math.min(Integer.parseInt(parts[1]), archives.size());
			} else if (part != null) {
				String[] parts = part.split("/");
				int partNo = Integer.parseInt(parts[0]);
				int partCo = Integer.parseInt(parts[1]);
				int partSize = (int) Math.ceil(archives.size() / partCo);
				start = (partNo - 1) * partSize;
				end = Math.min(partNo * partSize, archives.size());
			}
			
			queue.add(TextQueue.joinLines(archives.subList(start, end)));
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
