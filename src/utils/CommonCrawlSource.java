package utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.archive.io.ArchiveReader;
import org.archive.io.warc.WARCReaderFactory;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

public class CommonCrawlSource {
	
	public final static String BUCKET_NAME = "commoncrawl";
	public final static String STORAGE_URL = "s3.amazonaws.com";
	public final static String STORAGE_PATH = "crawl-data";
	
	public final static CommonCrawlSource DEFAULT = new CommonCrawlSource("CC-MAIN-2020-05");
	
	private final String crawl;
	
	public CommonCrawlSource(String crawl) {
		this.crawl = crawl;
	}
	
	public static enum WARC_TYPE {
		WARC, WET, WAT;

		@Override
		public String toString() {
			return this.name().toLowerCase();
		}
	}

	public static String getWarcName(String name) {
		int dotIndex = name.indexOf(".");
		if (dotIndex < 0) {
			return name;
		} else {
			return name.substring(0, dotIndex);
		}
	}

	public String getWarcPath(String segment, String name, WARC_TYPE type) {
		String typeEnding = ".warc";
		if (type != WARC_TYPE.WARC) {
			typeEnding += "." + type;
		}
		return String.join("/", STORAGE_PATH, this.crawl, "segments", segment, type.toString(), name + typeEnding + ".gz");
	}

	public String getWarcPath(String segment, String name) {
		return this.getWarcPath(segment, name, WARC_TYPE.WARC);
	}

	public String getWarcUrl(String segment, String name, WARC_TYPE type) {
		String domain = "https://" + BUCKET_NAME + "." + STORAGE_URL + "/";
		return domain + this.getWarcPath(segment, name, type);
	}

	public String getWarcUrl(String segment, String name) {
		return this.getWarcUrl(segment, name, WARC_TYPE.WARC);
	}
	
	public static String getFileNameFromUrl(String url) {
		return url.substring(url.lastIndexOf("/") + 1).split("\\.")[0];
	}
	
	public ArchiveReader getArchive(String segment, String name, WARC_TYPE type) throws IOException, ServiceException {
		S3Service s3s = new RestS3Service(null);
		String path = getWarcPath(segment, name, type);
		S3Object f = s3s.getObject(BUCKET_NAME, path, null, null, null, null, null, null);
		return WARCReaderFactory.get(path, f.getDataInputStream(), true);
	}
	
	public static ArchiveReader archiveFromURL(String urlString) throws MalformedURLException, IOException {
		URL url = new URL(urlString);
		return WARCReaderFactory.get(urlString, new BufferedInputStream(url.openStream()), true);
	}
	
	public List<String> archiveList(WARC_TYPE type, boolean addDomain) {
		List<String> archives = new ArrayList<String>();
		String urlString = "https://" + String.join("/", STORAGE_URL, BUCKET_NAME, STORAGE_PATH, this.crawl, type.name().toLowerCase() + ".paths.gz");
		try {
			URL url = new URL(urlString);
			BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(url.openStream())));
			String domain = addDomain ? "https://" + BUCKET_NAME + "." + STORAGE_URL + "/" : "";
			String line;
			while ((line = reader.readLine()) != null) {
				archives.add(domain + line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return archives;
	}

}
