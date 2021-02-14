package tasks;

import java.io.File;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import queue.Queue;
import queue.QueueElement;
import queue.TextQueue;

public class MoveBackQueue {
	
	private final static Options options;
	
	static {
		options = new Options();
		options.addOption("q", "queuePath", true, "Path to TextQeue directory.");
	}
	
	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);

			String queuePath = cmd.getOptionValue("q");
			
			Queue<String> queue = new TextQueue(new File(queuePath));
			
			QueueElement<String>[] elements = queue.getPending();
			for (int i = elements.length - 1; i >= 0; i--) {
				queue.back(elements[i].element, true);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
