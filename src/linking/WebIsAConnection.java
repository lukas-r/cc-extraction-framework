package linking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.StringUtils;
import extraction.BasicExtractor;
import extraction.pattern.Term;

public class WebIsAConnection implements LinkingConnection {
	
	enum EntityType {
		CLASS, INSTANCE
	}
	
	private final static String TUPLE_TABLE_PREFIX = "i";
	
	private StanfordCoreNLP pipeline;
	
	private final int tupleCount;
	private MongoClient client;
	private MongoDatabase db;
	
	public WebIsAConnection(String host, int port, String dbName, int tupleCount) {
		this.client = new MongoClient(host, port);
		this.db = client.getDatabase(dbName);
		this.tupleCount = tupleCount;
	}
	
	private StanfordCoreNLP getPipeline() {
		if (this.pipeline == null) {
			this.pipeline = BasicExtractor.getPipeline();
		}
		return this.pipeline;
	}

	public static String getTableNo(String entity) {
		return StringUtils.padLeft(entity.replaceAll("[^a-z]", "").substring(0, 2), 2, '0');
	}
	
	public Term getTerm(String entity) {
		Term term = this.getTermByTokex(entity);
		return term == null ? this.getTermConservatively(entity) : term;
	}
	
	public Term getTermByTokex(String entity) {
		CoreDocument document = new CoreDocument(entity);
		this.getPipeline().annotate(document);
		Term term = null;
		try {
			term = new Term(document.tokens());
		} catch (Exception e) {}
		return term;
	}
	
	private enum State {
		PREMOD, NOUNS, POSTMOD
	}
	
	public static String getWordString(List<CoreLabel> tokens) {
		return String.join(" ", tokens.stream().map(t -> t.word()).collect(Collectors.toList()));
	}
	
	public static String getLemmaString(List<CoreLabel> tokens) {
		return String.join(" ", tokens.stream().map(t -> t.lemma()).collect(Collectors.toList()));
	}
	
	public static String[] getTags(List<CoreLabel> tokens) {
		return tokens.stream().map(t -> t.tag()).toArray(String[]::new);
	}
	
	public Term getTermConservatively(String entity) {
		CoreDocument document = new CoreDocument(entity);
		this.pipeline.annotate(document);
		HashMap<State, List<CoreLabel>> lists = new HashMap<State, List<CoreLabel>>();
		for (State state: State.values()) {
			lists.put(state, new ArrayList<CoreLabel>());
		}
		State state = State.PREMOD;
		for (CoreLabel token: document.tokens()) {
			switch (state) {
				case PREMOD:
					if (token.tag().startsWith("NN") && !token.word().endsWith("'s") && !token.word().endsWith("s'")) {
						state = State.NOUNS;
					}
					break;
				case NOUNS:
					if (!token.tag().startsWith("NN")) {
						state = State.POSTMOD;
					}
					break;
				default:
					break;
			}
			lists.get(state).add(token);
		}
		return new Term(
			getWordString(lists.get(State.PREMOD)),
			getLemmaString(lists.get(State.PREMOD)),
			getTags(lists.get(State.PREMOD)),
			getWordString(lists.get(State.NOUNS)),
			getLemmaString(lists.get(State.NOUNS)),
			getTags(lists.get(State.NOUNS)),
			getWordString(lists.get(State.POSTMOD)),
			getTags(lists.get(State.POSTMOD))
		);
	}
	
	@Override
	public Object findEntity(String entity) throws Exception {
		String key = this.findEntity(entity, EntityType.INSTANCE);
		if (key == null) {
			key = this.findEntity(entity, EntityType.CLASS);
		}
		return key;
	}
	
	public String findEntity(String entity, EntityType type) {
		Term term = getTerm(entity);
		String typeLetter = type.name().substring(0, 1).toLowerCase();
		try {
			String collectionName = typeLetter + getTableNo(term.nouns);
			BasicDBObject query = new BasicDBObject();
			query.put(type.name().toLowerCase(), term.nouns);
			query.put("modifications." + typeLetter + "premod", term.preMod);
			query.put("modifications." + typeLetter + "postmod", term.postMod);
			BasicDBObject projection = new BasicDBObject();
			projection.put("modifications.$", 1);
			Document doc = this.db.getCollection(collectionName).find(query).projection(projection).first();
			if (doc != null) {
				return entity;
			}
		} catch (Exception e) {}
		return null;
	}

	@Override
	public Object findTuple(Object instanceEntity, Object classEntity) throws Exception {
		Term instanceTerm = getTerm((String) instanceEntity);
		String collectionName = WebIsAConnection.TUPLE_TABLE_PREFIX + getTableNo((String) instanceTerm.nouns);
		Term classTerm = getTerm((String) classEntity);
		BasicDBObject query = new BasicDBObject();
		query.put("instance", instanceTerm.nouns);
		query.put("modifications.ipremod", instanceTerm.preMod);
		query.put("modifications.ipostmod", instanceTerm.postMod);
		query.put("class", classTerm.nouns);
		query.put("modifications.cpremod", classTerm.preMod);
		query.put("modifications.cpostmod", classTerm.postMod);
		BasicDBObject projection = new BasicDBObject();
		projection.put("modifications.$", 1);
		Document doc = this.db.getCollection(collectionName).find(query).projection(projection).first();
		if (doc != null) {
			return doc.get("_id", ObjectId.class).toHexString() + "_" + doc.getList("modifications", Document.class).get(0).getLong("_id");
		}
		return null;
	}

	@Override
	public void close() {
		this.client.close();
	}
	
	public static void main(String[] args) {
		WebIsAConnection connection;
		try {
			connection = new WebIsAConnection("localhost", 27017, "testdb", 1);
			System.out.println(connection.findEntity("holy scriptures"));
			System.out.println(connection.findEntity("source of great joy"));
			System.out.println(connection.findTuple("source of great joy", "holy scriptures"));
			Iterator<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>> it = connection.tuples().iterator();
			System.out.println(it.next());
			System.out.println(it.next());
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	@Override
	public Iterable<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>> tuples() throws Exception {
		return new Iterable<Pair<Object,Pair<Pair<Object, String>, Pair<Object, String>>>>() {
			
			@Override
			public Iterator<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>> iterator() {
				return new TupleIterator();
			}
		};
	}
	
	private class TupleIterator implements Iterator<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>> {
		Iterator<String> tableIterator;
		Iterator<Document> tupleIterator;
		Iterator<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>> modificationIterator;
		
		public TupleIterator() {
			this.tableIterator = StreamSupport.stream(WebIsAConnection.this.db.listCollectionNames().spliterator(), false).filter(c -> c.startsWith(WebIsAConnection.TUPLE_TABLE_PREFIX)).iterator();
			this.nextTable();
		}
		
		private void nextTable() {
			this.tupleIterator = WebIsAConnection.this.db.getCollection(this.tableIterator.next()).find().iterator();
			this.nextTuple();
		}
		
		private void nextTuple() {
			Document currentTuple = this.tupleIterator.next();
			while(currentTuple.getList("modifications", Document.class).size() == 0) {
				if (!this.tupleIterator.hasNext()) {
					this.tupleIterator = WebIsAConnection.this.db.getCollection(this.tableIterator.next()).find().iterator();
				}
				currentTuple = this.tupleIterator.next();
			}
			Document tuple = currentTuple;
			this.modificationIterator = tuple.getList("modifications", Document.class).stream().map(m -> {
				String instanceString = (m.getString("ipremod") + " " + tuple.getString("instance") + " " + m.getString("ipostmod")).trim();
				String classString = (m.getString("cpremod") + " " + tuple.getString("class") + " " + m.getString("cpostmod")).trim();
				return Pair.of(
						(Object) (tuple.get("_id", ObjectId.class).toHexString() + "_" + m.getLong("_id")),
						Pair.of(Pair.of((Object) instanceString, instanceString), Pair.of((Object) classString, classString))
				);
			}).iterator();
		}

		@Override
		public boolean hasNext() {
			return this.modificationIterator.hasNext() || this.tupleIterator.hasNext() || this.tableIterator.hasNext();
		}

		@Override
		public Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>> next() {
			if (!modificationIterator.hasNext()) {
				if (tupleIterator.hasNext()) {
					this.nextTuple();
				} else {
					this.nextTable();
				}
			}
			return this.modificationIterator.next();
		}
		
	}

	@Override
	public int tupleCount() {
		return this.tupleCount;
	}

	@Override
	public String[] getTupleAttributes(String instanceString, String classString, Object instanceKey, Object classKey, Object tupleKey) {
		final Function<String, String> getSentence = (key) -> {
			String collectionName = "s" + Integer.valueOf(key) / 1000000;
			BasicDBObject query = new BasicDBObject();
			query.put("provid", key);
			BasicDBObject projection = new BasicDBObject();
			projection.put("sentence", 1);
			return this
					.db
					.getCollection(collectionName)
					.find(query)
					.projection(projection)
					.first()
					.getString("sentence");
		};
		String[] keyParts = ((String) tupleKey).split("_");
		String collectionName = WebIsAConnection.TUPLE_TABLE_PREFIX + getTableNo(this.getTerm(instanceString).nouns);
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(keyParts[0]));
		query.put("modifications._id", Long.valueOf(keyParts[1]));
		BasicDBObject projection = new BasicDBObject();
		projection.put("instance", 1);
		projection.put("class", 1);
		projection.put("modifications.$", 1);
		Document doc = null;
		try {			
			doc = this.db.getCollection(collectionName).find(query).projection(projection).first();
		} catch (Exception e) {
			while (true) {
				try {
					Thread.sleep(10000);
					this.db.listCollections();
					return new String[] {};
				} catch (Exception e2) {
					e.printStackTrace();
				}
			}
		}
		if (doc == null) {
			System.out.println("");
		}
		Document modification = doc.getList("modifications", Document.class).get(0);
		List<Document> sources = modification.getList("sources", Document.class);
		return new String[]{
			instanceString,
			classString,
			doc.getString("instance"),
			doc.getString("class"),
			String.valueOf(sources.size()),
			String.valueOf(sources.stream().map(d -> d.getString("pid")).collect(Collectors.toSet()).size()),
			String.valueOf(sources.stream().map(d -> d.getString("pld")).collect(Collectors.toSet()).size()),
			sources.stream().map(d -> getSentence.apply(d.getString("provid"))).collect(Collectors.joining(LinkingConnection.POOL_SEPARATOR)),
			sources.stream().map(d -> String.valueOf(d.getInteger("ipos"))).collect(Collectors.joining(LinkingConnection.POOL_SEPARATOR)),
			sources.stream().map(d -> String.valueOf(d.getInteger("cpos"))).collect(Collectors.joining(LinkingConnection.POOL_SEPARATOR)),
			"",
			"",
			"",
			"",
			"",
			"",
			String.valueOf(sources.stream().mapToInt(d -> d.get("iwiki", "").length() > 0 ? 1 : 0).sum()),
			String.valueOf(sources.stream().mapToInt(d -> d.get("cwiki", "").length() > 0 ? 1 : 0).sum())
		};
	}
	
}
