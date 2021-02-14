package extraction.output;

import java.util.LinkedHashMap;

public class SimpleOutputObjectFactory {
	String[] attributes;
	
	public SimpleOutputObjectFactory(String[] attributes) {
		this.attributes = attributes;
	}
	
	public OutputObject createObject(String... data) throws Exception {
		if (data.length != attributes.length) {
			throw new Exception("Wrong argument attribute number");
		}
		LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
		for (int i = 0; i < data.length; i++) {
			map.put(this.attributes[i], data[i]);
		}
		return new SimpleOutputObject(attributes, map);
	}

}
