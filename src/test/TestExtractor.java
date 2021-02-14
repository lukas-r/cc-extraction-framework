package test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.webdatacommons.isadb.util.Helper;

import extraction.BasicExtractor;
import extraction.EFWExtractor;
import extraction.Extractor;
import extraction.logging.ConsoleLogger;
import extraction.logging.Logger;
import extraction.output.TextOutputHandler;
import extraction.output.OutputHandler;
import utils.CommonCrawlSource;
import utils.Utils;

public class TestExtractor {
	
	public static void countDuplicates() {
		final Charset CHARSET = StandardCharsets.UTF_8;
		
		final int MAX_SENTENCE_LENGTH = 400;
		final int MIN_SENTENCE_LENGTH = 10;
		String inputFile = "C:\\Users\\Lukas\\Desktop\\work\\testfiles\\CC-MAIN-20200117123339-20200117151339-00153.warc.wet.gz";
		ArchiveReader archive;
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		try {
			archive = WARCReaderFactory.get(new File(inputFile));
			int i = 0;
			for (ArchiveRecord record: archive) {
				String url = record.getHeader().getUrl();
				String pld = url != null ? Utils.getPLD(url) : null;
				BufferedReader br = new BufferedReader(new InputStreamReader(new BufferedInputStream(record), CHARSET));
				String line;
				try {
					while ((line = br.readLine()) != null) {
						if (line.length() < MIN_SENTENCE_LENGTH) {
							continue;
						}
						ArrayList<String> sentences = Helper.filterSentences(Helper.splitLineToSentences(line),
								MIN_SENTENCE_LENGTH, MAX_SENTENCE_LENGTH);
						
						for (String sentence: sentences) {
							if (i++ % (1000*100) == 0) System.out.println(i);
							Set<String> set = map.get(sentence);
							if (set == null) {
								set = new HashSet<String>();
								map.put(sentence, set);
							}
							set.add(pld);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println();
		System.out.println(map.size());
		List<ImmutablePair<String, Integer>> duplicates = map.entrySet().stream().map(e -> new ImmutablePair<String, Integer>(e.getKey(), e.getValue().size())).filter(p -> p.right > 1).sorted((p1, p2) -> p2.right - p1.right).collect(Collectors.toList());
		System.out.println(duplicates.stream().count());
		System.out.println(duplicates.stream().map(p -> p.right).reduce(0, (a, b) -> a + b));
		System.out.println(String.join("\n", duplicates.stream().limit(1000).map(p -> p.right + " " + p.left).collect(Collectors.toList())));
	}
	
	public static void testHashing() {
		final Charset CHARSET = StandardCharsets.UTF_8;
		String string = "test";
		int hash1 = MurmurHash3.hash32x86(string.getBytes(CHARSET));
		int hash2 = MurmurHash3.hash32x86(new StringBuilder(string).reverse().toString().getBytes(CHARSET));
		long result = (long) hash1 << 32 | hash2;
		long result2 = hash1 << 32 | hash2;
		System.out.println(Integer.toBinaryString(0));
		System.out.println(Integer.toBinaryString(1));
		System.out.println(Integer.toBinaryString(-1));
		System.out.println(Integer.toBinaryString(Integer.MIN_VALUE));
		System.out.println(Integer.toBinaryString(Integer.MAX_VALUE));
		System.out.println();
		System.out.println(Integer.toBinaryString(hash1));
		System.out.println(Integer.toBinaryString(hash2));
		System.out.println(Long.toBinaryString(result));
		System.out.println(Long.toBinaryString(result2));
	}
	
	public static void testBasicExtractor() {
		//String inputFile = "C:\\Users\\Lukas\\Desktop\\work\\testfiles\\CC-MAIN-20200804102630-20200804132630-00103.warc.wet.gz";
		//File outputFile = new File("output-enhanced-" + new File(inputFile).getName() + ".txt");
		//OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(outputFile));
		// ByteArrayOutputStream os = new ByteArrayOutputStream();
		// OutputStreamWriter osw = new OutputStreamWriter(os);
		//BufferedWriter bw = new BufferedWriter(osw);
		try {
			//ArchiveReader archive = WARCReaderFactory.get(new File(inputFile));
			ArchiveReader archive = CommonCrawlSource.archiveFromURL("https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735916.91/wet/CC-MAIN-20200805065524-20200805095524-00291.warc.wet.gz");
			Extractor extractor = new BasicExtractor();
			Logger logger = new ConsoleLogger();
			OutputHandler output = new TextOutputHandler(logger, new OutputStreamWriter(System.out), false);
			System.out.println();
			System.out.println(Utils.mapFormat(extractor.extract(archive, output, logger), true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void testEFWExtractor() {
		String inputFile = "C:\\Users\\Lukas\\Desktop\\work\\testfiles\\CC-MAIN-20200117123339-20200117151339-00153.warc.wet.gz";
		try {
			Extractor extractor = new EFWExtractor();
			ArchiveReader archive = WARCReaderFactory.get(new File(inputFile));
			Logger logger = new ConsoleLogger();
			OutputHandler output = new TextOutputHandler(logger, new BufferedWriter(new OutputStreamWriter(System.out)), true);
			System.out.println(Utils.mapFormat(extractor.extract(archive, output, logger), true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		//countDuplicates();
		//testHashing();
		testBasicExtractor();
		//testEFWExtractor();
	}

}
