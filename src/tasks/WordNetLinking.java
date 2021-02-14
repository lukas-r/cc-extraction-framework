package tasks;

import java.io.File;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import db.PairType;
import linking.CommonDbConnection;
import linking.LinkingConnection;
import linking.WebIsAConnection;
import linking.WordNet;
import linking.WordNetLink;

public class WordNetLinking {
	
private final static Options options;
	
	static {
		options = new Options();
		options.addOption("w", "wordNetDb", true, "Path to WordNet database.");
		options.addOption("l", "linkingDb", true, "Path to linking database.");
		options.addOption("t", "linkingType", true, "Valid types are webisa and ccdb.");
		options.addOption("c", "connection", true, "Connection string for WebIsA mongo database. Schema: host:port/database/tuple-count");
		options.addOption("s", "sourceDb", true, "Path to source database.");
		options.addOption("r", "relationsDb", true, "Path to relations database.");
		options.addOption("p", "pairType", true, "Valid pair types are lemma and term.");
		options.addOption("m", "multiplier", true, "Multiplier for tuple sampling.");
		options.addOption("o", "csvOutput", true, "Output path for csv.");
		options.addOption("i", "includeTuples", false, "Include common tuples in csv.");
	}

	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
			
			String action = cmd.getArgs()[0];
			File wordNetDbFile = new File(cmd.getOptionValue("w"));
			File linkingDbFile = new File(cmd.getOptionValue("l"));
			
			if (action.equals("wordnet")) {
				WordNet.writeDb(wordNetDbFile);
			} else {
				String type = cmd.getOptionValue("t");			
				try {
					LinkingConnection connection = null;
					switch (type) {
					case "webisa":
						String connectionString = cmd.getOptionValue("c");
						String[] connectionParts = connectionString.split("[\\-\\/\\:\\\\]");
						String host = connectionParts[0];
						int port = Integer.parseInt(connectionParts[1]);
						String dbName = connectionParts[2];
						int tupleCount = Integer.parseInt(connectionParts[3]);
						connection = new WebIsAConnection(host, port, dbName, tupleCount);
						break;
					case "ccdb":
						PairType pairType = null;
						String pt = cmd.getOptionValue("p");
						if (pt.equals("term")) {
							pairType = PairType.TERM;
						} else if (type.equals("lemma")) {
							pairType = PairType.LEMMA;
						} else {
							throw new IllegalArgumentException();
						}
						File sourceDbFile = new File(cmd.getOptionValue("s"));
						File relationsDbFile = new File(cmd.getOptionValue("r"));
						connection = new CommonDbConnection(sourceDbFile, relationsDbFile, pairType);
						break;
					default:
						throw new IllegalArgumentException();
					}
					WordNetLink link = new WordNetLink(wordNetDbFile, linkingDbFile, connection);	
					
					if (!linkingDbFile.exists()) {
						link.findCommonEntities();
						link.findCommonTuples();
					}
					switch (action) {
					case "iterate":
						link.findAllTuplesByIterating();
						break;
					case "cross":
						link.findAllTuplesWithCrossProduct();
					case "sample":
						double multiplier = Double.parseDouble(cmd.getOptionValue("m"));
						link.sampleNegativeTuples(multiplier);
						break;
					case "csv":
						File csvFile = new File(cmd.getOptionValue("c"));
						boolean includeTupleTable = cmd.hasOption("i");
						link.writeCSV(csvFile, includeTupleTable);
						break;
					default:
						throw new IllegalArgumentException();
					}
				} catch (Exception e) {
					if (e instanceof IllegalArgumentException) {
						throw new RuntimeException(e);
					}
					e.printStackTrace();
				} finally {
					
				}
			}			
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
