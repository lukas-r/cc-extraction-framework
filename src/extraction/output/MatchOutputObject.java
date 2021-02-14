package extraction.output;

import java.util.LinkedHashMap;

public class MatchOutputObject implements OutputObject {
	
	public static final String TYPE = "M";
	public final String part;
	public final int position;
	
	private final LinkedHashMap<String, String> data;
	
	public MatchOutputObject(String sentencePart, int position) {
		this.data = new LinkedHashMap<String, String>();
		this.data.put("type", TYPE);
		this.part = sentencePart;
		this.data.put("part", this.part);
		this.position = position;
		this.data.put("position", Integer.toString(this.position));
	}

	@Override
	public String[] getAttributes() {
		return new String[] {"type", "part", "position"};
	}

	@Override
	public LinkedHashMap<String, String> getData() {
		return this.data;
	}

	public static MatchOutputObject fromLineParts(String ...parts) {
		return new MatchOutputObject(parts[1], Integer.parseInt(parts[2]));
	}
	
}
