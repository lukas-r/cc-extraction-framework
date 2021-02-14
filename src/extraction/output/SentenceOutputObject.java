package extraction.output;

import java.util.LinkedHashMap;

import extraction.pattern.Pattern;

public class SentenceOutputObject implements OutputObject {
	
	public static final String TYPE = "S";
	public final String pattern;
	public final String sentence;
	public final String pld;
	public final String url;
	
	private final LinkedHashMap<String, String> data;
	
	public SentenceOutputObject(Pattern pattern, String sentence, String pld, String url) {
		this(pattern.name, sentence, pld, url);
	}
	
	public SentenceOutputObject(String pattern, String sentence, String pld, String url) {
		this.data = new LinkedHashMap<String, String>();
		this.data.put("type", TYPE);
		this.pattern = pattern;
		this.data.put("pattern", this.pattern);
		this.sentence = sentence;
		this.data.put("sentence", this.sentence);
		this.pld = pld;
		this.data.put("pld", this.pld);
		this.url = url;
		this.data.put("url", this.url);
	}

	@Override
	public String[] getAttributes() {
		return new String[] {"type", "pattern", "sentence", "pld", "url"};
	}

	@Override
	public LinkedHashMap<String, String> getData() {
		return this.data;
	}
	
	public static SentenceOutputObject fromLineParts(String ...parts) {
		return new SentenceOutputObject(parts[1], parts[2], parts[3], parts[4]);
	}

}
