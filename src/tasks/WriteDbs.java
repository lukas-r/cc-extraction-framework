package tasks;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import db.ExtractionInserter;
import queue.Queue;
import queue.TextQueue;
import runner.Runner;

public class WriteDbs {
	
private final static Options options;
	
	static {
		options = new Options();
		options.addOption("q", "queuePath", true, "Path to TextQeue directory.");
		options.addOption("o", "dbPath", true, "Path to save dbs.");
		options.addOption("c", "crawl", true, "Name of the crawl.");
		options.addOption("t", "threads", true, "Number of threads and dbs to use.");
		options.addOption("z", "zip", false, "Whether input is gzipped.");
	}

	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
			
			String queuePath = cmd.getOptionValue("q");
			String dbDirStr = cmd.getOptionValue("o");
			String crawlName = cmd.getOptionValue("c");
			int threadNo = Integer.parseInt(cmd.getOptionValue("t"));
			boolean zipped = cmd.hasOption("z");
			
			Queue<String> queue = new TextQueue(new File(queuePath));
			Map<String, Integer> idToDb = new HashMap<String, Integer>();
			File dbDir = new File(dbDirStr);
			File stopFile = ExtractionInserter.getStopFile(dbDir);
			Runner runner = new Runner(threadNo, (id) -> {}, (id) -> {
				Integer dbId = idToDb.get(id);
				if (dbId == null) {
					dbId = idToDb.size();
					idToDb.put(id, dbId);
				}
				String dbPath = dbDir.getAbsolutePath() + File.separator + "db" + dbId + ".sqlite";
				while (queue.getTodo().length > 0) {
					ExtractionInserter inserter = null;
					try {
						inserter = new ExtractionInserter(new File(dbPath), queue, zipped);
						inserter.insert(crawlName);
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						try {							
							inserter.close();
						} catch (Exception e2) {
							e2.printStackTrace();
						}
					}
				}
			}, (id) -> {}, () -> stopFile.exists() || queue.getTodo().length == 0);
			runner.run();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
