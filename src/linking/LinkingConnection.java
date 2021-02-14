package linking;

import org.apache.commons.lang3.tuple.Pair;

public interface LinkingConnection {
	
	public final static String POOL_SEPARATOR = "|";
	public final String[] ATTRIBUTES = {
			"instance",
			"class",
			"instance_base",
			"class_base",
			"frequency",
			"pid_spread",
			"pld_spread",
			"sources_sentences",
			"sources_instance_pos",
			"sources_class_pos",
			"sources_instance_no",
			"sources_class_no",
			"sources_instance_depth",
			"sources_class_depth",
			"sources_instance_combined",
			"sources_class_combined",
			"sources_instance_wiki",
			"sources_class_wiki"
		};

	public Object findEntity(String entity) throws Exception;
	
	public Object findTuple(Object instanceEntity, Object classEntity) throws Exception;
	
	public Iterable<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>> tuples() throws Exception;
	
	public int tupleCount() throws Exception;
	
	public String[] getTupleAttributes(String instanceString, String classString, Object instanceKey, Object classKey, Object tupleKey) throws Exception;
	
	public void close();
	
}
