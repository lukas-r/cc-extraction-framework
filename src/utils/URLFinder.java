package utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import utils.CommonCrawlSource.WARC_TYPE;

public class URLFinder {
	
	public final static String WIKIPEDIA_REGEX = "^https?:\\/\\/([a-z]{1,4}).wikipedia.org\\/wiki\\/(.+)$";
	public final static Pattern WIKIPEDIA_PATTERN = Pattern.compile(WIKIPEDIA_REGEX);

	Iterator<ArchiveRecord> iterator;
	ArchiveRecord currentWat;

	public URLFinder(CommonCrawlSource source, String segment, String warcName) throws Exception {
		try {
			ArchiveReader reader = source.getArchive(segment, warcName, WARC_TYPE.WAT);

			iterator = reader.iterator();
			next();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void next() throws Exception {
		while (iterator.hasNext()) {
			currentWat = iterator.next();
			if (currentWat.getHeader().getHeaderValue(WARCRecord.HEADER_KEY_TYPE).equals("metadata")) {
				return;
			}
		}
		throw new Exception("End of File");
	}

	public static String read(InputStream input) {
		BufferedReader br = new BufferedReader(new InputStreamReader(new BufferedInputStream(input)));
		StringBuilder string = new StringBuilder();
		String line;
		try {
			while ((line = br.readLine()) != null) {
				string.append(line);
				string.append("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return string.toString();
	}

	private static Map<String, Set<String>> getLinksFromJson(String json, boolean wikiLinksOnly) {
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		JSONParser parser = new JSONParser();
		JSONObject root;
		try {
			root = (JSONObject) parser.parse(json);
			JSONObject envelope = (JSONObject) root.get("Envelope");
			JSONObject payload = (JSONObject) envelope.get("Payload-Metadata");
			JSONObject response = (JSONObject) payload.get("HTTP-Response-Metadata");
			JSONObject html = (JSONObject) response.get("HTML-Metadata");
			JSONArray links = (JSONArray) html.get("Links");
			for (Object link: links) {
				JSONObject linkObject = (JSONObject) link;
				if (linkObject.containsKey("text") && linkObject.containsKey("url")) {
					String text = (String) linkObject.get("text");
					String url = (String) linkObject.get("url");
					if (wikiLinksOnly) {
						Matcher matcher = URLFinder.WIKIPEDIA_PATTERN.matcher(url);
						if (matcher.find()) {
							url = matcher.group(1) + "/" + matcher.group(2);
						} else {
							continue;
						}
					}
					if (!map.containsKey(text)) {
						map.put(text, new HashSet<String>());
					}
					map.get(text).add(url);
				}
			}
		} catch (ParseException | NullPointerException e) {
			//e.printStackTrace();
		}
		return map;
	}

	public Map<String, Set<String>> getLinks(String recordId, boolean wikiLinkOnly) {
		try {
			while (!currentWat.getHeader().getHeaderValue(WARCRecord.HEADER_KEY_REFERS_TO).equals(recordId)) {
				next();
			}
			String record = read(currentWat);
			return getLinksFromJson(record, wikiLinkOnly);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		String name = CommonCrawlSource.getWarcName("CC-MAIN-20200117123339-20200117151339-00000.warc.wet.gz");
		try {
			URLFinder fetcher = new URLFinder(CommonCrawlSource.DEFAULT, "1579250589560.16", name);
			System.out.println(fetcher.getLinks("<urn:uuid:84b02cb0-897f-439b-8c11-844597c7bdf0>", true));
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println(getRequestResponseMap());
	}

}
