package tasks;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import db.PairType;
import taxonomy.TaxonomyCreator;

public class CreateTaxonomy {
	
private final static Options options;
	
	static {
		options = new Options();
		options.addOption("h", "helper", true, "Path to the helper db input.");
		options.addOption("o", "taxonomyDb", true, "Path to the taxonomy db output.");
		options.addOption("p", "pairType", true, "Optional pair type to create taxonomy for.");
		options.addOption("t", "tmp", true, "Temporary directory to use.");
	}

	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
			
			File helperDb = new File(cmd.getOptionValue("h"));
			File taxonomyDb = new File(cmd.getOptionValue("o"));
			File tmpDir = cmd.hasOption("t") ? new File(cmd.getOptionValue("t")) : null;
			
			PairType pairType = null;
			try {
				pairType = PairType.valueOf(cmd.getOptionValue("p"));
			} catch (Exception IllegalArgumentException) {}
			
			try {
				TaxonomyCreator creator = new TaxonomyCreator(taxonomyDb, helperDb, tmpDir);
				if (pairType == null) {
					creator.create();
				} else {
					creator.create(pairType);
				}
				creator.close();
			} catch (IOException | SQLException | InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}			
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
