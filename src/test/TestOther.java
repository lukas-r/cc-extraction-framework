package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

import queue.Queue;
import queue.TextQueue;
import utils.CommonCrawlSource;
import utils.QueueUtils;
import utils.CommonCrawlSource.WARC_TYPE;
import utils.Utils;

public class TestOther {
	
	public static void testCamel() {
		System.out.println(Utils.toCamelCase("Hallo_hiEr_istEinTest_da"));
	}
	
	public static int getCrawlSize(String name) {
		return new CommonCrawlSource(name).archiveList(WARC_TYPE.WET, false).size();
	}
	
	public static void crawlGraphStatistics() {
		try {
			URL url = new URL("https://commoncrawl.s3.amazonaws.com/projects/hyperlinkgraph/cc-main-2020-feb-mar-may/host/cc-main-2020-feb-mar-may-host-ranks.txt.gz");
			BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(url.openStream())));
			for (int i = 0; i < 10; i++) {
				System.out.println(reader.readLine());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	public static void writeRemaingTodos() {
		Set<String> done = new HashSet<String>();
		done.addAll(Lists.newArrayList(new File("D:\\done").list()));
		
		List<String> todos = new CommonCrawlSource("CC-MAIN-2020-34").archiveList(WARC_TYPE.WET, true).subList(00000, 60000);
		
		QueueUtils.getRemaingTodos(new ArrayList<String>(done), todos, new File("C:\\Users\\Lukas\\Desktop\\todos.txt"));
	}
	
	public static int getArchivePos(String archiveName, String crawl) {
		List<String> archives = new CommonCrawlSource(crawl).archiveList(WARC_TYPE.WET, false);
		archiveName = archiveName.split("\\.")[0];
		for (int i = 0; i < archives.size(); i++) {
			String[] parts = archives.get(i).split("/");
			String ca = parts[parts.length - 1].split("\\.")[0];
			if (ca.equals(archiveName)) {
				return i;
			}
		}
		return -1;
	}
	
	public static void moveValidZips(File source, File dest) {
		for (String file: Utils.getInvalidGzipFiles(source, true)) {
			try {
				Files.move(new File(source.getAbsolutePath() + File.separator + file), new File(dest.getAbsolutePath() + File.separator + file));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void testDownload() {
		File tmp;
		try {
			tmp = File.createTempFile("abc", "def");
			System.out.println(tmp.getAbsolutePath());
			tmp.deleteOnExit();
			URL url = new URL("https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735916.91/wet/CC-MAIN-20200805065524-20200805095524-00291.warc.wet.gz");
			Utils.download(url, tmp);
			tmp.delete();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void writeFilesToQueue(Queue<String> queue, File dir) {
		for (File file: dir.listFiles()) {
			if (file.isFile()) {
				queue.add(file.getAbsolutePath());
			}
		}
	}
	
	public static void main(String[] args) {
		//testCamel();
		//System.out.println(getCrawlSize());
		//writeRemaingTodos();
		//System.out.println(QueueUtils.getFileDifference(new File("C:\\Users\\Lukas\\Desktop\\m2\\todos.txt"), new File("C:\\Users\\Lukas\\Desktop\\m2\\queue\\todo.txt")));
		//System.out.println(Utils.getInvalidGzipFiles(new File("C:\\Users\\Lukas\\Desktop\\sm\\m1\\output\\"), false));
		//System.out.println(getArchivePos("CC-MAIN-20200811180239-20200811210239-00284", "CC-MAIN-2020-34"));
		//moveValidZips(new File("C:\\Users\\Lukas\\Desktop\\sm\\m2\\output"), new File("C:\\Users\\Lukas\\Desktop\\sm\\m2\\done"));
		//testDownload();
		writeFilesToQueue(new TextQueue(new File("C:\\Users\\Lukas\\Desktop\\db\\queue")), new File("E:\\mt\\done"));
	}

}
