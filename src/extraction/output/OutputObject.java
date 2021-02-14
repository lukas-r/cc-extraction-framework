package extraction.output;

import java.util.LinkedHashMap;

public interface OutputObject {
	
	public String[] getAttributes();
	
	public LinkedHashMap<String, String> getData();

}
