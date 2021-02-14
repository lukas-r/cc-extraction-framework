package tasks;

import java.io.File;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import db.Merger;

public class MergeDbs {

private final static Options options;
	
	static {
		options = new Options();
		options.addOption("db1", "firstDb", true, "Path to first db.");
		options.addOption("db2", "secondDb", true, "Path to second db.");
		options.addOption("o", "mergedDb", true, "Path where to create merged db.");
		options.addOption("t", "tmp", true, "Optional temporary directory to use.");
	}

	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
			
			File firstDb = new File(cmd.getOptionValue("db1"));
			File secondDb = new File(cmd.getOptionValue("db2"));
			File mergedDb = new File(cmd.getOptionValue("o"));
			File tmpDir = cmd.hasOption("t") ? new File(cmd.getOptionValue("t")) : null;
			
			Merger merger = new Merger(firstDb, secondDb, mergedDb, tmpDir);
			merger.merge();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
}
