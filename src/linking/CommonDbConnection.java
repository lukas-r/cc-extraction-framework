package linking;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import db.DbConnection;
import db.PairType;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import extraction.BasicExtractor;
import extraction.pattern.Term;
import utils.DbUtils;
import utils.Utils;

public class CommonDbConnection implements LinkingConnection {
	
	private DbConnection sourceDb;
	private DbConnection relationsDb;
	private PairType pairType;
	
	private StanfordCoreNLP pipeline;
	
	private PreparedStatement findLemma;
	private PreparedStatement findTerm;
	private PreparedStatement findTuple;
	private PreparedStatement getAllTuples;
	private PreparedStatement getEntityString;
	private PreparedStatement getTupleCount;
	private PreparedStatement getMatchingInformations;
	private PreparedStatement getMatching;
	private PreparedStatement getInstance;
	private PreparedStatement getClass;
	
	public CommonDbConnection(File sourceDbFile, File relationsDbFile, PairType pairType) throws SQLException {
		this.sourceDb = new DbConnection(sourceDbFile, true) {};
		this.relationsDb = new DbConnection(relationsDbFile, true) {};
		this.pairType = pairType;
		this.prepareStatements();
	}
	
	private StanfordCoreNLP getPipeline() {
		if (this.pipeline == null) {
			this.pipeline = BasicExtractor.getPipeline();
		}
		return this.pipeline;
	}
	
	private void prepareStatements() throws SQLException {
		if (this.pairType == PairType.LEMMA) {
			this.findLemma = this.sourceDb.connection.prepareStatement("SELECT id FROM lemmagroup WHERE words = ?");
		} else {
			this.findTerm = this.sourceDb.connection.prepareStatement("SELECT id FROM term WHERE term.premod = (SELECT id FROM premod WHERE words = ?) AND term.noungroup = (SELECT id FROM noungroup WHERE words = ?) AND term.postmod = (SELECT id FROM postmod WHERE words = ?)");
		}
		this.findTuple = this.relationsDb.connection.prepareStatement("SELECT id, count, pld_count FROM " + (this.pairType == PairType.LEMMA ? "lemma" : "term") + "_pair WHERE instance = ? AND class = ?");
		this.getAllTuples = this.relationsDb.connection.prepareStatement("SELECT instance, class FROM " + (this.pairType == PairType.LEMMA ? "lemma" : "term") + "_pair");
		this.getTupleCount = this.relationsDb.connection.prepareStatement("SELECT COUNT(*) FROM " + (this.pairType == PairType.LEMMA ? "lemma" : "term") + "_pair");
		if (this.pairType == PairType.LEMMA) {
			this.getEntityString = this.sourceDb.connection.prepareStatement("SELECT words FROM lemmagroup WHERE id = ?");
		} else {
			this.getEntityString = this.sourceDb.connection.prepareStatement("SELECT id,  pre.words || \" \" + noun.words \" \" + post.words FROM term AS t, premod AS pre, noungroup AS noun, postmod AS post WHERE t.id = ? AND t.premod = pre.id AND t.noungroup = noun.id AND t.postmod = post.id");
		}
		this.getMatchingInformations = this.relationsDb.connection.prepareStatement("SELECT matching FROM matching_connection WHERE " + this.pairType.toString().toLowerCase() + "_pair = ?");
		this.getMatching = this.sourceDb.connection.prepareStatement("SELECT m.pid, s.sentence FROM matching AS m, sentence AS s WHERE m.id = ? AND s.id = m.sentence");
		this.getInstance = this.sourceDb.connection.prepareStatement("SELECT no, pos, depth, combined FROM instance WHERE matching = ? AND " + (this.pairType == PairType.TERM ? "term" : "lemmagroup") + " = ?");
		this.getClass = this.sourceDb.connection.prepareStatement("SELECT no, pos, depth, combined FROM class WHERE matching = ? AND " + (this.pairType == PairType.TERM ? "term" : "lemmagroup") + " = ?");
	}

	@Override
	public Object findEntity(String entity) throws SQLException {
		if (this.pairType == PairType.LEMMA) {
			this.findLemma.setString(1, entity);
			ResultSet lemma = this.findLemma.executeQuery();
			if (lemma.next()) {
				return lemma.getInt(1);
			}
		} else {
			CoreDocument document = new CoreDocument(entity);
			this.getPipeline().annotate(document);
			try {				
				Term term = new Term(document.tokens());
				this.findTerm.setString(1, term.preMod);
				this.findTerm.setString(2, term.nouns);
				this.findTerm.setString(3, term.postMod);
				ResultSet termResult = this.findTerm.executeQuery();
				if (termResult.next()) {
					return termResult.getInt(1);
				}
			} catch (Exception e) {}
		}
		return null;
	}

	@Override
	public Object findTuple(Object instanceEntity, Object classEntity) throws SQLException {
		this.findTuple.setInt(1, (int) instanceEntity);
		this.findTuple.setInt(2, (int) classEntity);
		ResultSet tuple = this.findTuple.executeQuery();
		if (tuple.next()) {
			return tuple.getInt(1);
		}
		return null;
	}

	@Override
	public void close() {
		this.sourceDb.close();
		this.relationsDb.close();
	}
	
	public static void main(String[] args) {
		try {
			LinkingConnection connection = new CommonDbConnection(new File("C:\\Users\\Lukas\\Desktop\\work\\wordnet\\db.sqlite"), new File("C:\\Users\\Lukas\\Desktop\\work\\wordnet\\relations.sqlite"), PairType.LEMMA);
			Iterator<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>> it = connection.tuples().iterator();
			System.out.println(it.next());
			System.out.println(it.next());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Iterable<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>> tuples() throws SQLException {
		ResultSet tuples = this.getAllTuples.executeQuery();
		return new Iterable<Pair<Object,Pair<Pair<Object, String>, Pair<Object, String>>>>() {
			
			@Override
			public Iterator<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>> iterator() {
				return new Iterator<Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>>>() {

					@Override
					public boolean hasNext() {
						try {
							return !tuples.isLast();
						} catch (SQLException e) {
							e.printStackTrace();
						}
						return false;
					}

					@Override
					public Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>> next() {
						try {
							tuples.next();
							int id = tuples.getInt(1);
							int instanceEntityId = tuples.getInt(2);
							int classEntityId = tuples.getInt(2);
							CommonDbConnection.this.getEntityString.setInt(1, instanceEntityId);
							String instanceString = DbUtils.getFirstRow(CommonDbConnection.this.getEntityString.executeQuery()).getString(1);
							CommonDbConnection.this.getEntityString.setInt(1, classEntityId);
							String classString = DbUtils.getFirstRow(CommonDbConnection.this.getEntityString.executeQuery()).getString(1);
							return Pair.of(id, Pair.of(Pair.of(instanceEntityId, instanceString), Pair.of(classEntityId, classString)));
						} catch (SQLException e) {
							e.printStackTrace();
						}
						return null;
					}
				};
			}
		};
		
	}

	@Override
	public int tupleCount() throws SQLException {
		return DbUtils.getFirstRow(this.getTupleCount.executeQuery()).getInt(1);
	}

	@Override
	public String[] getTupleAttributes(String instanceString, String classString, Object instanceKey, Object classKey, Object tupleKey) throws SQLException{
		String instanceBase = "";
		String classBase = "";
		if (this.pairType == PairType.TERM) {			
			try {				
				CoreDocument document = new CoreDocument(instanceString);
				this.getPipeline().annotate(document);
				instanceBase = new Term(document.tokens()).nouns;
			} catch (Exception e) {}
			try {				
				CoreDocument document = new CoreDocument(classString);
				this.getPipeline().annotate(document);
				classBase = new Term(document.tokens()).nouns;
			} catch (Exception e) {}
		}
		
		findTuple.setString(1, instanceString);
		findTuple.setString(2, classString);
		ResultSet tuple = DbUtils.getFirstRow(findTuple.executeQuery());
		long tupleId = tuple.getLong(1);
		int frequency = tuple.getInt(2);
		int pld_spread = tuple.getInt(3);
		
		Set<Integer> pids = new HashSet<Integer>();
		List<String> sentences = new ArrayList<String>();
		List<Integer> instancePos = new ArrayList<Integer>();
		List<Integer> classPos = new ArrayList<Integer>();
		List<Integer> instanceNo = new ArrayList<Integer>();
		List<Integer> classNo = new ArrayList<Integer>();
		List<Integer> instanceDepth = new ArrayList<Integer>();
		List<Integer> classDepth = new ArrayList<Integer>();
		List<Boolean> instanceCombined = new ArrayList<Boolean>();
		List<Boolean> classCombined = new ArrayList<Boolean>();
		
		this.getInstance.setObject(2, instanceKey);
		this.getClass.setObject(2, classKey);
		this.getMatchingInformations.setLong(1, tupleId);
		ResultSet matchingInformations = this.getMatchingInformations.executeQuery();
		while (matchingInformations.next()) {
			long matchingId = matchingInformations.getLong(1);
			this.getMatching.setLong(1, matchingId);
			ResultSet matching = DbUtils.getFirstRow(this.getMatching.executeQuery());
			pids.add(matching.getInt(1));
			sentences.add(matching.getString(2));
			this.getInstance.setLong(1, matchingId);
			this.getClass.setLong(1, matchingId);
			ResultSet instanceEntity = DbUtils.getFirstRow(this.getInstance.executeQuery());
			instancePos.add(instanceEntity.getInt(2));
			instanceNo.add(instanceEntity.getInt(1));
			instanceDepth.add(instanceEntity.getInt(3));
			instanceCombined.add(instanceEntity.getBoolean(4));
			ResultSet classEntity = DbUtils.getFirstRow(this.getClass.executeQuery());
			classPos.add(classEntity.getInt(2));
			classNo.add(classEntity.getInt(1));
			classDepth.add(classEntity.getInt(3));
			classCombined.add(classEntity.getBoolean(4));
		}
		return new String[] {
				instanceString,
				classString,
				instanceBase,
				classBase,
				String.valueOf(frequency),
				String.valueOf(pids.size()),
				String.valueOf(pld_spread),
				String.join(LinkingConnection.POOL_SEPARATOR, sentences),
				instancePos.stream().map(e -> e.toString()).collect(Collectors.joining(POOL_SEPARATOR)),
				classPos.stream().map(e -> e.toString()).collect(Collectors.joining(POOL_SEPARATOR)),
				instanceNo.stream().map(e -> e.toString()).collect(Collectors.joining(POOL_SEPARATOR)),
				classNo.stream().map(e -> e.toString()).collect(Collectors.joining(POOL_SEPARATOR)),
				instanceDepth.stream().map(e -> e.toString()).collect(Collectors.joining(POOL_SEPARATOR)),
				classDepth.stream().map(e -> e.toString()).collect(Collectors.joining(POOL_SEPARATOR)),
				instanceCombined.stream().map(e -> String.valueOf(Utils.intFromBool(e))).collect(Collectors.joining(POOL_SEPARATOR)),
				classCombined.stream().map(e -> String.valueOf(Utils.intFromBool(e))).collect(Collectors.joining(POOL_SEPARATOR)),
				"",
				""
		};
	}
	
}
