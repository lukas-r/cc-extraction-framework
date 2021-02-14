package extraction.output;

import java.util.LinkedHashMap;

public class SimpleOutputObject implements OutputObject {
	private final String[] attributes;
	private final LinkedHashMap<String, String> data;
	
	public SimpleOutputObject(String[] attributes, LinkedHashMap<String, String> data) {
		this.attributes = attributes;
		this.data = data;
	}

	@Override
	public String[] getAttributes() {
		return this.attributes;
	}

	@Override
	public LinkedHashMap<String, String> getData() {
		return this.data;
	}

}
