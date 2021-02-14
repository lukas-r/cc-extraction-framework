package tasks;

import java.io.File;
import java.sql.SQLException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import db.RelationIterator;
import db.RelationIterator.Step;

public class IterateCandidates {
	
private final static Options options;
	
	static {
		options = new Options();
		options.addOption("c", "crawlDb", true, "Path to the crawl db input.");
		options.addOption("r", "relationsDb", true, "Path to the relations db output.");
		options.addOption("h", "helperDb", true, "Path to the helper db output.");
	}

	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
			
			String crawlDbPath = cmd.getOptionValue("c");
			String relationsDbPath = cmd.getOptionValue("r");
			String helperDbPath = cmd.getOptionValue("h");
			
			RelationIterator iterator = new RelationIterator(new File(crawlDbPath), new File(relationsDbPath), new File(helperDbPath));
			try {
				iterator.iterate(Step.PREPARE, true);
				iterator.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
