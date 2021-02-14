package extraction.output;

import java.util.LinkedHashMap;

public class DuplicateOutputObject implements OutputObject {
	
	public static final String TYPE = "D";
	public final String sentence;
	public final String pld;
	public final String url;
	
	private final LinkedHashMap<String, String> data;
	
	public DuplicateOutputObject(String sentence, String pld, String url) {
		this.data = new LinkedHashMap<String, String>();
		this.data.put("type", TYPE);
		this.sentence = sentence;
		this.data.put("sentence", this.sentence);
		this.pld = pld;
		this.data.put("pld", this.pld);
		this.url = url;
		this.data.put("url", this.url);
	}

	@Override
	public String[] getAttributes() {
		return new String[] {"type", "sentence", "pld", "url"};
	}

	@Override
	public LinkedHashMap<String, String> getData() {
		return data;
	}
	
	public static DuplicateOutputObject fromLineParts(String ...parts) {
		return new DuplicateOutputObject(parts[1], parts[2], parts[3]);
	}

}
