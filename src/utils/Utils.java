package utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.net.InternetDomainName;

public abstract class Utils {
	
	public static String getDomain(String url) {
		String tmpUrl = url.replaceAll("https?://", "");
		if (tmpUrl.contains("/"))
			tmpUrl = tmpUrl.substring(0, tmpUrl.indexOf("/"));
		if (tmpUrl.contains("?"))
			tmpUrl = tmpUrl.substring(0, tmpUrl.indexOf("?"));

		if (tmpUrl.contains("@")) {
			tmpUrl = tmpUrl.substring(tmpUrl.indexOf("@") + 1);
		} else if (tmpUrl.contains(":")) {
			tmpUrl = tmpUrl.substring(0, tmpUrl.indexOf(":"));
		}
		return tmpUrl;
	}
	
	public static String getPLD(String url) throws IllegalStateException, IllegalArgumentException {
		String tmpUrl = getDomain(url);
		try {
			tmpUrl = InternetDomainName.from(tmpUrl).topPrivateDomain().toString();
		} catch (IllegalStateException | IllegalArgumentException e) {
			throw e;
		}
		return tmpUrl;
	}
	
	public static String mapFormat(Map<String, String> map, boolean rightAlignValues) {
		Optional<Integer> maxKeyLen = map.keySet().stream().map(k -> k.length()).max((a, b) -> a - b);
		Optional<Integer> maxValLen = map.values().stream().flatMap(v -> Lists.newArrayList(v.split("\\r?\\n")).stream()).map(v -> v.length()).max((a, b) -> a - b);
		return formatStringPairs(map.entrySet(), maxKeyLen.get(), maxValLen.get(), rightAlignValues);
	}
	
	public static String entriesFormat(Collection<Entry<String, String>> entries, boolean rightAlignValues) {
		Optional<Integer> maxKeyLen = entries.stream().map(e -> e.getKey().length()).max((a, b) -> a - b);
		Optional<Integer> maxValLen = entries.stream().flatMap(e -> Lists.newArrayList(e.getValue().split("\\r?\\n")).stream()).map(v -> v.length()).max((a, b) -> a - b);
		return formatStringPairs(entries, maxKeyLen.get(), maxValLen.get(), rightAlignValues);
	}
	
	private static String formatStringPairs(Collection<Entry<String, String>> pairs, int maxKeyLen, int maxValLen, boolean rightAlignValues) {
		return pairs.stream().map(e -> e.getKey() + ":" + Strings.repeat(" ", maxKeyLen - e.getKey().length() + 1) + (rightAlignValues ? Strings.repeat(" ", maxValLen - e.getValue().split("\\r?\\n")[0].length()) : "") + e.getValue()).collect(Collectors.joining("\n"));
	}
	
	public static String getHostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return "UNKOWN";
	}
	

	public static String toCamelCase(String string) {
		StringBuilder camel = new StringBuilder();
		for (String part: string.split("(_|(?<=\\p{Ll})(?=\\p{Lu}))")) {
			if (camel.length() == 0) {
				camel.append(part.toLowerCase());
			} else {
				camel.append(part.substring(0, 1).toUpperCase());
				camel.append(part.substring(1).toLowerCase());
			}
		}
		return camel.toString();
	}
	
	public static boolean isValidGzip(File file) {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
			while (reader.readLine() != null);
			reader.close();
			return true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
		}
		return false;
	}
	
	public static List<String> getInvalidGzipFiles(File directory, boolean invert) {
		File[] files = directory.listFiles();
		int lastP = -1;
		List<String> invalidFiles = new ArrayList<String>();
		for (int i = 0; i < files.length; i++) {
			if (i * 100 / files.length > lastP) {
				lastP = i * 100 / files.length;
				System.out.println(lastP + "%");
			}
			if (files[i].isFile()) {
				if (!isValidGzip(files[i]) ^ invert) {
					invalidFiles.add(files[i].getName());
				}
			}
		}
		System.out.println("100%");
		return invalidFiles;
	}
	
	public static void writeLines(Iterable<String> lines, File output) {
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(output));
			for (String line: lines) {
				writer.write(line);
				writer.newLine();
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void download(URL source, File dest) throws IOException {
		URLConnection connection = source.openConnection();
		InputStream input = new BufferedInputStream(connection.getInputStream());
		OutputStream output = new BufferedOutputStream(new FileOutputStream(dest));
		byte[] buffer = new byte[4096];
		int length;
		while ((length = input.read(buffer)) > 0) {
			output.write(buffer, 0, length);
		}
		output.close();
		input.close();
	}
	
	public static int intFromBool(boolean b) {
		return b ? 1 : 0;
	}

}
