package db;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import utils.DbUtils;
import utils.measures.EnumTimer;

public class RelationIterator {
	
	public enum Step {
		PREPARE, WRITE_CANDIDATES, ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ANY
	}

	private enum Count {
		ONE, MANY, UNDEFINED
	}
	
	private enum TimerElements {
		ITERATE,
		PREPARE,
		WRITE_CANDIDATES,
		ONE_TO_ONE,
		ONE_TO_MANY,
		MANY_TO_MANY,
		QUERY_CANDIDATES,
		ITERATE_CANDIDATES,
		FIND_MATCHES,
		FIND_PLDS,
		DETERMINE_CLASS,
		DETERMINE_INSTANCES,
		INSERT_MATCHES,
		QUERY_PLDS,
		ITERATE_PLDS,
		ADD_PLD,
		INSERT_LEMMA_NODE_PARENT,
		INSERT_TERM_NODE_PARENT,
		INSERT_LEMMA_NODE_CHILD,
		INSERT_TERM_NODE_CHILD,
		INSERT_LEMMA_PAIR,
		INSERT_TERM_PAIR,
		DC_GET_CLASS_PROB,
		DC_GET_PAIR_PROB,
		TEST
	}
	
	private DbConnection crawlDb;
	private PairConnection pairDb;
	private HelperConnection helperDb;
	
	
	private PreparedStatement candidateSetDoneQuery;
	
	private PreparedStatement sentenceQuery;
	private PreparedStatement pldQuery;
	private PreparedStatement instanceQuery;
	private PreparedStatement classQuery;
	private PreparedStatement lemmaQuery;

	private PreparedStatement pairMatchingConnectionInsertQuery;
	private PreparedStatement nodeMatchingConnectionInsertQuery;
	
	private PreparedStatement pldLemmaNodeInsertQuery;
	private PreparedStatement pldTermNodeInsertQuery;
	private PreparedStatement pldLemmaPairInsertQuery;
	private PreparedStatement pldTermPairInsertQuery;
	
	private PreparedStatement pldLemmaNodeSelectQuery;
	
	private PreparedStatement lemmaNodeSelectQuery;
	private PreparedStatement termNodeSelectQuery;
	private PreparedStatement lemmaPairSelectQuery;
	private PreparedStatement termPairSelectQuery;
	
	private PreparedStatement lemmaNodeInsertQuery;
	private PreparedStatement termNodeInsertQuery;
	private PreparedStatement lemmaPairInsertQuery;
	private PreparedStatement termPairInsertQuery;
	
	private PreparedStatement lemmaNodeUpdateQuery;
	private PreparedStatement termNodeUpdateQuery;
	private PreparedStatement lemmaPairUpdateQuery;
	private PreparedStatement termPairUpdateQuery;
	
	private PreparedStatement sumClassLemmaCountQuery;
	private PreparedStatement lemmagroupByIdQuery;
	private PreparedStatement lemmagroupByLemmaQuery;
	private PreparedStatement lemmaNodesByParentQuery;
	
	private EnumTimer<TimerElements> timer;
	
	public RelationIterator(File crawlDb, File pairDb, File helperDb) {		
		this.crawlDb = new DbConnection(crawlDb, true) {};
		this.pairDb = new PairConnection(pairDb);
		this.helperDb = new HelperConnection(helperDb);
		this.timer = new EnumTimer<TimerElements>(TimerElements.class);
	}
	
	public void iterate(int maxIterations) {
		this.close();
	}
	
	public void iterate(Step step, boolean forward) throws SQLException {
		this.timer.start(TimerElements.ITERATE);
		this.pairDb.connection.setAutoCommit(false);
		switch (step) {
			case PREPARE:
				this.timer.start(TimerElements.PREPARE);
				prepare();
				this.timer.stop(TimerElements.PREPARE);
				if (!forward) break;
			case WRITE_CANDIDATES:
				this.timer.start(TimerElements.WRITE_CANDIDATES);
				writeCandidateList();
				this.timer.stop(TimerElements.WRITE_CANDIDATES);
				if (!forward) break;
			case ONE_TO_ONE:
				this.timer.start(TimerElements.ONE_TO_ONE);
				writeCandidates(Count.ONE, Count.ONE);
				this.timer.stop(TimerElements.ONE_TO_ONE);
				if (!forward) break;
			case ONE_TO_MANY:
				this.timer.start(TimerElements.ONE_TO_MANY);
				writeCandidates(Count.ONE, Count.MANY);
				this.timer.stop(TimerElements.ONE_TO_MANY);
				if (!forward) break;
			case MANY_TO_ANY:
				this.timer.start(TimerElements.MANY_TO_MANY);
				writeCandidates(Count.MANY, Count.UNDEFINED);
				this.timer.stop(TimerElements.MANY_TO_MANY);
				if (!forward) break;
			default:
				break;
		}
		this.timer.stop(TimerElements.ITERATE);
	}
	
	public void close() {
		this.crawlDb.close();
		this.pairDb.close();
	}
	
	private void prepare() {
		
	}
	
	private void writeCandidateList() throws SQLException {
		PreparedStatement insertMatchingCandidate = this.helperDb.connection.prepareStatement("INSERT INTO matching_candidate (matching, class_count, instance_count) VALUES (?, ?, ?)");
		ResultSet matchings = this.crawlDb.connection.createStatement().executeQuery("SELECT matching, instance_count, class_count FROM matching_info");
		int i = 0;
		while (matchings.next()) {
			if (i % 1000000 == 0) {
				System.out.println("write candidate " + i);
			}
			i++;
			int matchingId = matchings.getInt(1);
			int instanceCount = matchings.getInt(2);
			int classCount = matchings.getInt(3);
			insertMatchingCandidate.setInt(1, matchingId);
			insertMatchingCandidate.setInt(2, classCount);
			insertMatchingCandidate.setInt(3, instanceCount);
			insertMatchingCandidate.execute();
		}
		insertMatchingCandidate.close();
		this.helperDb.connection.commit();
	}
	
	private static int getInsertedKey(Statement statement) throws SQLException {
		return DbUtils.getFirstRow(statement.getGeneratedKeys()).getInt(1);
	}
	
	private int insertLemmaPair(Set<Integer> plds, int instanceId, int classId) throws SQLException {
		this.lemmaPairSelectQuery.setInt(1, instanceId);
		this.lemmaPairSelectQuery.setInt(2, classId);
		ResultSet lemmaPair = this.lemmaPairSelectQuery.executeQuery();
		int lemmaPairId;
		if (lemmaPair.next()) {
			lemmaPairId = lemmaPair.getInt(1);
			int newPlds = this.insertPlds(this.pldLemmaPairInsertQuery, plds, lemmaPairId);
			this.lemmaPairUpdateQuery.setInt(1, plds.size());
			this.lemmaPairUpdateQuery.setInt(2, newPlds);
			this.lemmaPairUpdateQuery.setInt(3, lemmaPairId);
			this.lemmaPairUpdateQuery.execute();
		} else {
			this.lemmaPairInsertQuery.setInt(1, instanceId);
			this.lemmaPairInsertQuery.setInt(2, classId);
			this.lemmaPairInsertQuery.setInt(3, plds.size());
			this.lemmaPairInsertQuery.setInt(4, plds.size());
			this.lemmaPairInsertQuery.execute();
			lemmaPairId = getInsertedKey(this.lemmaPairInsertQuery);
			this.insertPlds(this.pldLemmaPairInsertQuery, plds, lemmaPairId);
		}
		return lemmaPairId;
	}
	
	private int insertTermPair(Set<Integer> plds, int instanceId, int classId, int lemmaPairId) throws SQLException {
		this.termPairSelectQuery.setInt(1, instanceId);
		this.termPairSelectQuery.setInt(2, classId);
		ResultSet termPair = this.termPairSelectQuery.executeQuery();
		int termPairId;
		if (termPair.next()) {
			termPairId = termPair.getInt(1);
			int newPlds = this.insertPlds(this.pldTermPairInsertQuery, plds, termPairId);
			this.termPairUpdateQuery.setInt(1, plds.size());
			this.termPairUpdateQuery.setInt(2, newPlds);
			this.termPairUpdateQuery.setInt(3, termPairId);
			this.termPairUpdateQuery.execute();
		} else {
			this.termPairInsertQuery.setInt(1, instanceId);
			this.termPairInsertQuery.setInt(2, classId);
			this.termPairInsertQuery.setInt(3, lemmaPairId);
			this.termPairInsertQuery.setInt(4, plds.size());
			this.termPairInsertQuery.setInt(5, plds.size());
			this.termPairInsertQuery.execute();
			termPairId = getInsertedKey(this.termPairInsertQuery);
			this.insertPlds(this.pldTermPairInsertQuery, plds, termPairId);
		}
		return termPairId;
	}
	
	private int insertLemmaNode(Set<Integer> plds, int lemmaId, int parentId, int parentLemmaId, String childrenLemmas, int childCount) throws SQLException {
		this.lemmaNodeSelectQuery.setInt(1, lemmaId);			
		this.lemmaNodeSelectQuery.setInt(2, parentId);
		this.lemmaNodeSelectQuery.setString(3, childrenLemmas);
		ResultSet lemmaNode = this.lemmaNodeSelectQuery.executeQuery();
		int lemmaNodeId;
		if (lemmaNode.next()) {
			lemmaNodeId = lemmaNode.getInt(1);
			int newPlds = this.insertPlds(this.pldLemmaNodeInsertQuery, plds, lemmaNodeId);
			this.lemmaNodeUpdateQuery.setInt(1, plds.size());
			this.lemmaNodeUpdateQuery.setInt(2, newPlds);
			this.lemmaNodeUpdateQuery.setInt(3, lemmaNodeId);
			this.lemmaNodeUpdateQuery.execute();
		} else {
			this.lemmaNodeInsertQuery.setInt(1, lemmaId);
			this.lemmaNodeInsertQuery.setInt(2, parentId);
			this.lemmaNodeInsertQuery.setInt(3, parentLemmaId);
			this.lemmaNodeInsertQuery.setString(4, childrenLemmas);
			this.lemmaNodeInsertQuery.setInt(5, childCount);
			this.lemmaNodeInsertQuery.setInt(6, plds.size());
			this.lemmaNodeInsertQuery.setInt(7, plds.size());
			this.lemmaNodeInsertQuery.execute();
			lemmaNodeId = getInsertedKey(this.lemmaNodeInsertQuery);
			this.insertPlds(this.pldLemmaNodeInsertQuery, plds, lemmaNodeId);
		}
		return lemmaNodeId;
	}
	
	private int insertTermNode(Set<Integer> plds, int termId, int parentId, int parentTermId, int lemmaNodeId, String childrenTerms, int childCount) throws SQLException {
		this.termNodeSelectQuery.setInt(1, termId);
		this.termNodeSelectQuery.setInt(2, parentId);
		this.termNodeSelectQuery.setString(3, childrenTerms);
		ResultSet termNode = this.termNodeSelectQuery.executeQuery();
		int termNodeId;
		if (termNode.next()) {
			termNodeId = termNode.getInt(1);
			int newPlds = this.insertPlds(this.pldTermNodeInsertQuery, plds, termNodeId);
			this.termNodeUpdateQuery.setInt(1, plds.size());
			this.termNodeUpdateQuery.setInt(2, newPlds);
			this.termNodeUpdateQuery.setInt(3, termNodeId);
			this.termNodeUpdateQuery.execute();
		} else {
			this.termNodeInsertQuery.setInt(1, termId);
			this.termNodeInsertQuery.setInt(2, lemmaNodeId);
			this.termNodeInsertQuery.setInt(3, parentId);
			this.termNodeInsertQuery.setInt(4, parentTermId);
			this.termNodeInsertQuery.setString(5, childrenTerms);
			this.termNodeInsertQuery.setInt(6, childCount);
			this.termNodeInsertQuery.setInt(7, plds.size());
			this.termNodeInsertQuery.setInt(8, plds.size());
			this.termNodeInsertQuery.execute();
			termNodeId = getInsertedKey(this.termNodeInsertQuery);
			this.insertPlds(this.pldTermNodeInsertQuery, plds, termNodeId);
		}
		return termNodeId;
	}
	
	private void insertMatchingConnection(int matchingId, int pldCount, int lemmaPairId, int termPairId, int lemmaNodeId, int termNodeId) throws SQLException {
		this.pairMatchingConnectionInsertQuery.setInt(1, matchingId);
		this.pairMatchingConnectionInsertQuery.setInt(2, lemmaPairId);
		this.pairMatchingConnectionInsertQuery.setInt(3, termPairId);
		this.pairMatchingConnectionInsertQuery.execute();
		
		this.nodeMatchingConnectionInsertQuery.setInt(1, matchingId);
		this.nodeMatchingConnectionInsertQuery.setInt(2, lemmaNodeId);
		this.nodeMatchingConnectionInsertQuery.setInt(3, termNodeId);
		this.nodeMatchingConnectionInsertQuery.execute();
	}
	
	private int insertPlds(PreparedStatement query, Set<Integer> plds, int entryId) throws SQLException {
		int newPlds = 0;
		for (int pld: plds) {
			query.setInt(1, entryId);
			query.setInt(2, pld);
			try {
				query.execute();
				newPlds++;
			} catch (SQLException e) {}
		}
		return newPlds;
	}
	
	@SuppressWarnings("unchecked")
	private void insertMatches(Set<Integer> plds, int matchingId, List<Match> instanceTerms, Match classTerm) throws SQLException {
		String childrenTerms = ";" + String.join(";;", instanceTerms.stream().map(t -> String.valueOf(t.termId)).sorted().collect(Collectors.toList())) + ";";
		String childrenLemmas = ";" + String.join(";;", instanceTerms.stream().map(t -> String.valueOf(t.lemmaId)).sorted().collect(Collectors.toList())) + ";";
		
		this.timer.start(TimerElements.INSERT_LEMMA_NODE_PARENT);
		int parentLemmaNode = insertLemmaNode(Collections.EMPTY_SET, classTerm.lemmaId, -1, -1, childrenLemmas, instanceTerms.size());
		this.timer.stop(TimerElements.INSERT_LEMMA_NODE_PARENT);
		this.timer.start(TimerElements.INSERT_TERM_NODE_PARENT);
		int parentTermNode = insertTermNode(Collections.EMPTY_SET, classTerm.termId, -1, -1, parentLemmaNode, childrenTerms, instanceTerms.size());
		this.timer.stop(TimerElements.INSERT_TERM_NODE_PARENT);
		
		Map<Integer, Pair<Integer, Integer>> lemmaEntries = new HashMap<Integer, Pair<Integer, Integer>>();
		
		for (Match instanceTerm: instanceTerms) {
			Pair<Integer, Integer> lemmaTermNodeIds = lemmaEntries.get(instanceTerm.lemmaId);
			
			int lemmaPair;
			if (lemmaTermNodeIds != null) {
				lemmaPair = lemmaTermNodeIds.getLeft();
			} else {
				this.timer.start(TimerElements.INSERT_LEMMA_PAIR);
				lemmaPair = insertLemmaPair(plds, instanceTerm.lemmaId, classTerm.lemmaId);
				this.timer.stop(TimerElements.INSERT_LEMMA_PAIR);
			}
			this.timer.start(TimerElements.INSERT_TERM_PAIR);
			int termPair = insertTermPair(plds, instanceTerm.termId, classTerm.termId, lemmaPair);
			this.timer.stop(TimerElements.INSERT_TERM_PAIR);
			
			int childLemmaNode;
			if (lemmaTermNodeIds != null) {
				childLemmaNode = lemmaTermNodeIds.getRight();
			} else {
				this.timer.start(TimerElements.INSERT_LEMMA_NODE_CHILD);
				childLemmaNode = insertLemmaNode(plds, instanceTerm.lemmaId, parentLemmaNode, classTerm.lemmaId, "", 0);
				this.timer.stop(TimerElements.INSERT_LEMMA_NODE_CHILD);
			}
			this.timer.start(TimerElements.INSERT_TERM_NODE_CHILD);
			int childTermNode = insertTermNode(plds, instanceTerm.termId, parentTermNode, classTerm.termId, childLemmaNode, "", 0);
			this.timer.stop(TimerElements.INSERT_TERM_NODE_CHILD);
			
			insertMatchingConnection(matchingId, plds.size(), lemmaPair, termPair, childLemmaNode, childTermNode);
			
			if (lemmaTermNodeIds == null) {
				lemmaEntries.put(instanceTerm.lemmaId, Pair.of(lemmaPair, childLemmaNode));
			}
		}
	}
	
	private static class Matches {
		public final List<Match> instances;
		public final List<Match> classes;
		
		public Matches(List<Match> instances, List<Match> classes) {
			this.instances = instances;
			this.classes = classes;
		}
	}
	
	private List<Match> getTerms(PreparedStatement query, int matchingId) throws SQLException {
		List<Match> terms = new ArrayList<Match>();
		
		query.setInt(1, matchingId);
		ResultSet instanceSet = query.executeQuery();
		while (instanceSet.next()) {
			int termId = instanceSet.getInt("term");
			int depth = instanceSet.getInt("depth");
			int no = instanceSet.getInt("no");
			int pos = instanceSet.getInt("pos");
			boolean combined = instanceSet.getBoolean("combined");
			this.lemmaQuery.setInt(1, termId);
			int lemmaId = DbUtils.getFirstRow(this.lemmaQuery.executeQuery()).getInt(1);
			
			if (combined) {
				this.lemmagroupByIdQuery.setInt(1, lemmaId);
				String lemma = DbUtils.getFirstRow(this.lemmagroupByIdQuery.executeQuery()).getString(1);
				String replacedLemma = lemma.replace("and", "&");
				if (!lemma.equals(replacedLemma)) {
					this.lemmagroupByLemmaQuery.setString(1, replacedLemma);
					ResultSet result = this.lemmagroupByLemmaQuery.executeQuery();
					if (result.next()) {
						lemmaId = result.getInt(1);
					}
				}
			}
			
			terms.add(new Match(termId, lemmaId, depth, no, pos, combined));
		}
		
		return terms;
	}
	
	private Set<Integer> getPlds(int sentenceId) throws SQLException {
		Set<Integer> plds = new HashSet<Integer>();
		this.pldQuery.setInt(1, sentenceId);
		this.timer.start(TimerElements.QUERY_PLDS);
		ResultSet pldSet = this.pldQuery.executeQuery();
		this.timer.start(TimerElements.QUERY_PLDS);
		this.timer.start(TimerElements.ITERATE_PLDS);
		while (pldSet.next()) {
			this.timer.stop(TimerElements.ITERATE_PLDS);
			this.timer.start(TimerElements.ADD_PLD);
			plds.add(pldSet.getInt(1));
			this.timer.stop(TimerElements.ADD_PLD);
			this.timer.start(TimerElements.ITERATE_PLDS);
		}
		this.timer.stop(TimerElements.ITERATE_PLDS);
		return plds;
	}
	
	private Matches matchesFromMatching(int matchingId) throws SQLException {
		List<Match> instances = this.getTerms(this.instanceQuery, matchingId);
		List<Match> classes = this.getTerms(this.classQuery, matchingId);
		return new Matches(instances, classes);
	}
	
	private static class Match {
		public final int termId;
		public final int lemmaId;
		public final int depth;
		public final int no;
		public final int pos;
		@SuppressWarnings("unused")
		public final boolean combined;
		
		public Match(int termId, int lemmaId, int depth, int no, int pos, boolean combined) {
			this.termId = termId;
			this.lemmaId = lemmaId;
			this.depth = depth;
			this.no = no;
			this.pos = pos;
			this.combined = combined;
		}
		
	}
	
	private String countToOperator(String column, Count count) {
		switch(count) {
			case MANY:
				return column + " > 1";
			case ONE:
				return column + " = 1";
			case UNDEFINED:
				return column + " > 0";
			default:
				return "";
		}
	}
	
	private String getWhereCondition(Count classCount, Count instanceCount) {
		StringBuilder condition = new StringBuilder();
		condition.append(" WHERE ");
		condition.append(countToOperator("class_count", classCount));
		condition.append(" AND ");
		condition.append(countToOperator("instance_count", instanceCount));
		condition.append(" AND ");
		condition.append("done = 0");
		return condition.toString();
	}
	
	private void openCandidateQueries() throws SQLException {
		this.candidateSetDoneQuery = this.helperDb.connection.prepareStatement("UPDATE matching_candidate SET done = 1 WHERE id = ?");
		
		this.sentenceQuery = this.crawlDb.connection.prepareStatement("SELECT sentence FROM matching WHERE id = ?");
		this.pldQuery = this.crawlDb.connection.prepareStatement("SELECT pld FROM page_sentence WHERE sentence = ?");
		this.instanceQuery = this.crawlDb.connection.prepareStatement("SELECT term, depth, no, pos, combined FROM instance WHERE matching = ?");
		this.classQuery = this.crawlDb.connection.prepareStatement("SELECT term, depth, no, pos, combined FROM class WHERE matching = ?");
		this.lemmaQuery = this.crawlDb.connection.prepareStatement("SELECT lemmas FROM noungroup WHERE id = (SELECT noungroup FROM term WHERE id = ?)");

		this.pairMatchingConnectionInsertQuery = this.pairDb.connection.prepareStatement("INSERT INTO matching_connection (matching, lemma_pair, term_pair) VALUES (?, ?, ?)");
		this.nodeMatchingConnectionInsertQuery = this.helperDb.connection.prepareStatement("INSERT INTO matching_connection (matching, lemma_node, term_node) VALUES (?, ?, ?)");
		
		this.pldLemmaNodeInsertQuery = this.helperDb.connection.prepareStatement("INSERT INTO lemma_node_pld (lemma_node, pld) VALUES (? ,?)");
		this.pldTermNodeInsertQuery = this.helperDb.connection.prepareStatement("INSERT INTO term_node_pld (term_node, pld) VALUES (? ,?)");
		this.pldLemmaPairInsertQuery = this.pairDb.connection.prepareStatement("INSERT INTO lemma_pair_pld (lemma_pair, pld) VALUES (? ,?)");
		this.pldTermPairInsertQuery = this.pairDb.connection.prepareStatement("INSERT INTO term_pair_pld (term_pair, pld) VALUES (? ,?)");
		
		this.pldLemmaNodeSelectQuery = this.helperDb.connection.prepareStatement("SELECT pld FROM lemma_node_pld WHERE lemma_node = ?");
		
		this.lemmaNodeSelectQuery = this.helperDb.connection.prepareStatement("SELECT id, count, pld_count FROM lemma_node WHERE lemma = ? AND parent = ? AND children_lemmas = ?");
		this.termNodeSelectQuery = this.helperDb.connection.prepareStatement("SELECT id, count, pld_count FROM term_node WHERE term = ? AND parent = ? AND children_terms = ?");
		this.lemmaPairSelectQuery = this.pairDb.connection.prepareStatement("SELECT id, count, pld_count FROM lemma_pair WHERE instance = ? AND class = ?");
		this.termPairSelectQuery = this.pairDb.connection.prepareStatement("SELECT id, count, pld_count FROM term_pair WHERE instance = ? AND class = ?");
		
		this.lemmaNodeInsertQuery = this.helperDb.connection.prepareStatement("INSERT INTO lemma_node (lemma, parent, parent_lemma, children_lemmas, child_count, count, pld_count) VALUES (?, ?, ?, ?, ?, ?, ?)");
		this.termNodeInsertQuery = this.helperDb.connection.prepareStatement("INSERT INTO term_node (term, lemma, parent, parent_term, children_terms, child_count, count, pld_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
		this.lemmaPairInsertQuery = this.pairDb.connection.prepareStatement("INSERT INTO lemma_pair (instance, class, count, pld_count) VALUES (?, ?, ?, ?)");
		this.termPairInsertQuery = this.pairDb.connection.prepareStatement("INSERT INTO term_pair (instance, class, lemma_pair, count, pld_count) VALUES (?, ?, ?, ?, ?)");
		
		this.lemmaNodeUpdateQuery = this.helperDb.connection.prepareStatement("UPDATE lemma_node SET count = count + ?, pld_count = pld_count + ? WHERE id = ?");
		this.termNodeUpdateQuery = this.helperDb.connection.prepareStatement("UPDATE term_node SET count = count + ?, pld_count = pld_count + ? WHERE id = ?");
		this.lemmaPairUpdateQuery = this.pairDb.connection.prepareStatement("UPDATE lemma_pair SET count = count + ?, pld_count = pld_count + ? WHERE id = ?");
		this.termPairUpdateQuery = this.pairDb.connection.prepareStatement("UPDATE term_pair SET count = count + ?, pld_count = pld_count + ? WHERE id = ?");
	
		this.sumClassLemmaCountQuery = this.pairDb.connection.prepareStatement("SELECT sum(count) as count, sum(pld_count) as pld_count FROM lemma_pair WHERE class = ?");
		this.lemmagroupByIdQuery = this.crawlDb.connection.prepareStatement("SELECT words FROM lemmagroup WHERE id = ?");
		this.lemmagroupByLemmaQuery = this.crawlDb.connection.prepareStatement("SELECT id FROM lemmagroup WHERE words = ?");
		this.lemmaNodesByParentQuery = this.helperDb.connection.prepareStatement("SELECT id, parent FROM lemma_node WHERE lemma = ? AND parent_lemma = ?");
	}
	
	private void closeCandidateQueries() throws SQLException {
		this.candidateSetDoneQuery.close();
		
		this.sentenceQuery.close();
		this.pldQuery.close();
		this.instanceQuery.close();
		this.classQuery.close();
		this.lemmaQuery.close();

		this.pairMatchingConnectionInsertQuery.close();
		this.nodeMatchingConnectionInsertQuery.close();
		
		this.pldLemmaNodeInsertQuery.close();
		this.pldTermNodeInsertQuery.close();
		this.pldLemmaPairInsertQuery.close();
		this.pldTermPairInsertQuery.close();
		
		this.pldLemmaNodeSelectQuery.close();
		
		this.lemmaNodeSelectQuery.close();
		this.termNodeSelectQuery.close();
		this.lemmaPairSelectQuery.close();
		this.termPairSelectQuery.close();
		
		this.lemmaNodeInsertQuery.close();
		this.termNodeInsertQuery.close();
		this.lemmaPairInsertQuery.close();
		this.termPairInsertQuery.close();
		
		this.lemmaNodeUpdateQuery.close();
		this.termNodeUpdateQuery.close();
		this.lemmaPairUpdateQuery.close();
		this.termPairUpdateQuery.close();
		
		this.sumClassLemmaCountQuery.close();
		this.lemmagroupByIdQuery.close();
		this.lemmagroupByLemmaQuery.close();
		this.lemmaNodesByParentQuery.close();
	}
	
	private Match determineClass(List<Match> candidates, List<Match> instances) throws SQLException {
		if (candidates.size() == 1) {
			return candidates.get(0);
		}
		Match classMatch = null;
		double maxRating = -1;
		
		double[][] probs = new double[candidates.size()][instances.size()];
		double[] classCount = new double[candidates.size()];
		double minProb = 1;
		double maxClassCount = 0;
		for (int c = 0; c < candidates.size(); c++) {
			this.sumClassLemmaCountQuery.setInt(1, candidates.get(c).lemmaId);
			this.timer.start(TimerElements.DC_GET_CLASS_PROB);
			classCount[c] = DbUtils.getFirstRow(this.sumClassLemmaCountQuery.executeQuery()).getInt("pld_count");
			this.timer.stop(TimerElements.DC_GET_CLASS_PROB);
			if (classCount[c] > maxClassCount) {
				maxClassCount = classCount[c];
			}
			for (int i = 0; i < instances.size(); i++) {
				this.lemmaPairSelectQuery.setInt(1, instances.get(i).lemmaId);
				this.lemmaPairSelectQuery.setInt(2, candidates.get(c).lemmaId);
				this.timer.start(TimerElements.DC_GET_PAIR_PROB);
				ResultSet lemmaPairResult = this.lemmaPairSelectQuery.executeQuery();
				this.timer.stop(TimerElements.DC_GET_PAIR_PROB);
				double pairCount = lemmaPairResult.next() ? lemmaPairResult.getInt("pld_count") : 0;
				probs[c][i] = classCount[c] > 0 ? pairCount / classCount[c] : 0;
				if (probs[c][i] < minProb && probs[c][i] != 0) {
					minProb = probs[c][i];
				}
			}
		}
		if (maxClassCount == 0) {
			return candidates.stream().sorted((c1, c2) -> c1.depth - c2.depth).findFirst().get();
		}
		double eps = Math.sqrt(minProb);
		for (int c = 0; c < candidates.size(); c++) {
			double rating = classCount[c];
			for (int i = 0; i < instances.size(); i++) {
				rating *= probs[c][i] == 0 ? eps : probs[c][i];
			}
			if (rating > maxRating) {
				maxRating = rating;
				classMatch = candidates.get(c);
			}
		}
		return classMatch;
	}
	
	private int getTripleCount(int instanceId1, int instanceId2, int classId) throws SQLException {
		Set<Integer> parents = new HashSet<Integer>();
		
		this.lemmaNodesByParentQuery.setInt(1, instanceId1);
		this.lemmaNodesByParentQuery.setInt(2, classId);
		ResultSet nodes = this.lemmaNodesByParentQuery.executeQuery();
		while (nodes.next()) {
			parents.add(nodes.getInt("parent"));
		}
		
		Set<Integer> plds = new HashSet<Integer>();
		
		this.lemmaNodesByParentQuery.setInt(1, instanceId2);
		this.lemmaNodesByParentQuery.setInt(2, classId);
		nodes = this.lemmaNodesByParentQuery.executeQuery();
		while(nodes.next()) {
			if (parents.remove(nodes.getInt("parent"))) {
				this.pldLemmaNodeSelectQuery.setInt(1, nodes.getInt("id"));
				ResultSet pldSet = this.pldLemmaNodeSelectQuery.executeQuery();
				while(pldSet.next()) {
					plds.add(pldSet.getInt("pld"));
				}
			}
		}
		return plds.size();
	}
	
	private List<Match> determineInstances(List<Match> candidates, Match classMatch) throws SQLException {
		if (candidates.size() == 1) {
			return candidates;
		}
		List<Pair<Match, Double>> instances = new ArrayList<Pair<Match, Double>>();
		
		Map<Integer, List<Match>> instancePos = new HashMap<Integer, List<Match>>();
		for (Match candidate: candidates) {
			List<Match> candidateList = instancePos.get(candidate.no);
			if (candidateList == null) {
				candidateList = new ArrayList<Match>();
				instancePos.put(candidate.no, candidateList);
			}
			candidateList.add(candidate);
		}
		
		for (int pos = 0; pos < instancePos.keySet().size(); pos++) {
			if (!instancePos.containsKey(pos)) {
				break;
			}
			int lastPos = instances.size() > 0 ? instances.get(instances.size() - 1).getLeft().pos : -1;
			List<Match> candidateList = instancePos.get(pos).stream().filter(c -> c.pos > lastPos).collect(Collectors.toList());
			if (candidateList.size() == 0) {
				break;
			}
			if (instances.size() > 0 && instances.get(instances.size() - 1).getLeft().combined && candidateList.size() == 1) {
				break;
			}
			
			Match instance = null;
			double maxRating = -1;
			
			double[][] probs = new double[candidateList.size()][instances.size()];
			double[] classCandidateCount = new double[candidateList.size()];
			double minProb = 1;
			double maxClassCandidateCount = 0;
			for (int c = 0; c < candidateList.size(); c++) {
				this.lemmaPairSelectQuery.setInt(1, candidateList.get(c).lemmaId);
				this.lemmaPairSelectQuery.setInt(2, classMatch.lemmaId);
				ResultSet lemmaPairResult = this.lemmaPairSelectQuery.executeQuery();
				classCandidateCount[c] = lemmaPairResult.next() ? lemmaPairResult.getInt(1) : 0;
				if (classCandidateCount[c] > maxClassCandidateCount) {
					maxClassCandidateCount = classCandidateCount[c];
				}
				for (int i = 0; i < instances.size(); i++) {
					double tripleCount = this.getTripleCount(candidateList.get(c).lemmaId, instances.get(i).getLeft().lemmaId, classMatch.lemmaId);
					probs[c][i] = classCandidateCount[c] > 0 ? tripleCount / classCandidateCount[c] : 0;
					if (probs[c][i] != 0 && probs[c][i] < minProb) {
						minProb = probs[c][i];
					}
				}
			}
			if (maxClassCandidateCount == 0) {
				if (instances.size() == 0) {
					instances.add(
						IntStream.range(0, candidateList.size())
						.mapToObj(i -> Pair.of(i, candidateList.get(i)))
						.sorted((c1, c2) -> c1.getRight().depth - c2.getRight().depth)
						.sorted((c1, c2) -> BooleanUtils.toInteger(c1.getRight().combined) - BooleanUtils.toInteger(c2.getRight().combined))
						.findFirst().map(p -> Pair.of(p.getRight(), 0D)).get()
					);
				}
				break;
			}
			double eps = Math.sqrt(minProb);
			for (int c = 0; c < candidateList.size(); c++) {
				double rating = classCandidateCount[c];
				for (int i = 0; i < instances.size(); i++) {
					rating *= probs[c][i] == 0 ? eps : probs[c][i];
				}
				if (rating > maxRating) {
					maxRating = rating;
					instance = candidateList.get(c);
				}
			}
			instances.add(Pair.of(instance, maxRating));
			if (instance.depth > 0) {
				break;
			}
		}
		for (int i = 0; i < instances.size() - 1; i++) {
			for (int j = i + 1; j < instances.size(); j++) {
				if (instances.get(i).getLeft().termId == instances.get(j).getLeft().termId) {
					instances.remove(j);
					j--;
				}
			}
		}
		return instances.stream().map(i -> i.getLeft()).collect(Collectors.toList());
	}
	
	private void writeCandidates(Count instanceCount, Count classCount) throws SQLException {
		openCandidateQueries();
		
		String whereCondition = getWhereCondition(classCount, instanceCount);
		int candidateCount = DbUtils.getFirstRow(this.helperDb.connection.createStatement().executeQuery("SELECT count(*) FROM matching_candidate" + whereCondition)).getInt(1);
		this.timer.start(TimerElements.QUERY_CANDIDATES);
		ResultSet candidates = this.helperDb.connection.createStatement().executeQuery("SELECT id, matching FROM matching_candidate" + whereCondition);
		int i = 0;
		this.timer.start(TimerElements.ITERATE_CANDIDATES);
		int mC = 0;
		int mI = 0;
		while (candidates.next()) {
			this.timer.stop(TimerElements.ITERATE_CANDIDATES);
			if (i % 100000 == 0) {
				System.out.println(String.format("write candidate %d %.2f%%", i, i * 100D / candidateCount));
				System.out.println("multiinstes " + mI + " class " + mC);
				System.out.println(this.timer);
			}
			i++;
			int id = candidates.getInt(1);
			int matchingId = candidates.getInt(2);
			
			this.sentenceQuery.setInt(1, matchingId);
			int sentenceId = DbUtils.getFirstRow(this.sentenceQuery.executeQuery()).getInt(1);
			this.timer.start(TimerElements.FIND_PLDS);
			Set<Integer> plds = this.getPlds(sentenceId);
			this.timer.stop(TimerElements.FIND_PLDS);
			
			this.timer.start(TimerElements.FIND_MATCHES);
			Matches matches = matchesFromMatching(matchingId);
			this.timer.stop(TimerElements.FIND_MATCHES);
			if (matches.instances.size() > 1) mI++;
			if (matches.classes.size() > 1) mC++;
			
			
			this.timer.start(TimerElements.DETERMINE_CLASS);
			Match classTerm = determineClass(matches.classes, matches.instances);
			this.timer.stop(TimerElements.DETERMINE_CLASS);
			
			this.timer.start(TimerElements.DETERMINE_INSTANCES);
			List<Match> instanceTerms = determineInstances(matches.instances, classTerm);
			this.timer.stop(TimerElements.DETERMINE_INSTANCES);
			if (instanceTerms.size() == 0) {
				continue;
			}
			
			this.timer.start(TimerElements.INSERT_MATCHES);
			insertMatches(plds, matchingId, instanceTerms, classTerm);
			this.timer.stop(TimerElements.INSERT_MATCHES);
			
			this.candidateSetDoneQuery.setInt(1, id);
			this.candidateSetDoneQuery.execute();
			if (i % 1000000 == 0) {
				this.pairDb.connection.commit();
				this.helperDb.connection.commit();
			}
			if (i % 10000000 == 0) {
				this.pairDb.connection.commit();
				this.helperDb.connection.commit();
				this.closeCandidateQueries();
				this.pairDb.close();
				this.helperDb.close();
				this.crawlDb = new DbConnection(crawlDb.dbFile, true) {};
				this.pairDb = new PairConnection(pairDb.dbFile);
				this.openCandidateQueries();
			}

			this.timer.start(TimerElements.ITERATE_CANDIDATES);
		}
		this.timer.stop(TimerElements.ITERATE_CANDIDATES);
		this.pairDb.connection.commit();
		this.helperDb.connection.commit();
		this.closeCandidateQueries();
	}
	
	private static class PairConnection extends DbConnection {

		public PairConnection(File dbFile) {
			super(dbFile, false);
		}
		
		@Override
		protected void createTables() {
			this.context.createTable("lemma_pair")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("instance", SQLDataType.INTEGER)
				.column("class", SQLDataType.INTEGER)
				.column("count", SQLDataType.INTEGER)//to parent
				.column("pld_count", SQLDataType.INTEGER)
				.constraints(
					DSL.unique("instance", "class")/*,
					DSL.foreignKey("instance").references("lemmagroup"),
					DSL.foreignKey("class").references("lemmagroup")*/
				)
				.execute();
			//this.context.createIndex().on("lemma_pair", "instance").execute();
			this.context.createIndex().on("lemma_pair", "class").execute();
			this.context.createIndex().on("lemma_pair", "class", "count", "pld_count").execute(); //for sum on class
			
			this.context.createTable("lemma_pair_pld")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("lemma_pair", SQLDataType.INTEGER)
				.column("pld", SQLDataType.INTEGER)
				.constraints(
					DSL.unique("lemma_pair", "pld"),
					DSL.foreignKey("lemma_pair").references("lemma_pair")/*,
					DSL.foreignKey("pld").references("pld")*/
				)
				.execute();
			this.context.createIndex().on("lemma_pair_pld", "lemma_pair").execute();
			
			this.context.createTable("term_pair")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("instance", SQLDataType.INTEGER)
				.column("class", SQLDataType.INTEGER)
				.column("count", SQLDataType.INTEGER)
				.column("pld_count", SQLDataType.INTEGER)
				.column("lemma_pair", SQLDataType.INTEGER)
				.constraints(
					DSL.unique("instance", "class")/*,
					DSL.foreignKey("instance").references("term"),
					DSL.foreignKey("class").references("term")*/,
					DSL.foreignKey("lemma_pair").references("lemma_pair")
				)
				.execute();
			//this.context.createIndex().on("term_pair", "instance").execute();
			this.context.createIndex().on("term_pair", "class").execute();
			//this.context.createIndex().on("term_pair", "lemma_pair").execute();
			
			this.context.createTable("term_pair_pld")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("term_pair", SQLDataType.INTEGER)
				.column("pld", SQLDataType.INTEGER)
				.constraints(
					DSL.unique("term_pair", "pld"),
					DSL.foreignKey("term_pair").references("term_pair")/*,
					DSL.foreignKey("pld").references("pld")*/
				)
				.execute();
			this.context.createIndex().on("term_pair_pld", "term_pair").execute();
			
			this.context.createTable("matching_connection")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("matching", SQLDataType.INTEGER)
				.column("lemma_pair", SQLDataType.INTEGER)
				.column("term_pair", SQLDataType.INTEGER)
				.constraints(
					/*DSL.foreignKey("matching").references("matching"),*/
					DSL.foreignKey("lemma_pair").references("lemma_pair"),
					DSL.foreignKey("term_pair").references("term_pair")
				)
				.execute();
			//this.context.createIndex().on("matching_connection", "matching").execute();
			/*this.context.createIndex().on("matching_connection", "lemma_pair").execute();
			this.context.createIndex().on("matching_connection", "term_pair").execute();*/
		}
	
		
		@Override
		protected void prepare() {
			try {
				super.connection.setAutoCommit(false);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private static class HelperConnection extends DbConnection {

		public HelperConnection(File dbFile) {
			super(dbFile, false);
		}
		
		@Override
		protected void createTables() {
			this.context.createTable("lemma_node")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("lemma", SQLDataType.INTEGER)
				.column("parent", SQLDataType.INTEGER)
				.column("parent_lemma", SQLDataType.INTEGER)
				.column("children_lemmas", SQLDataType.VARCHAR)
				.column("child_count", SQLDataType.INTEGER)
				.column("count", SQLDataType.INTEGER)//to parent
				.column("pld_count", SQLDataType.INTEGER)//to parent
				.constraints(
					DSL.unique("lemma", "parent", "children_lemmas")/*,
					DSL.foreignKey("lemma").references("lemma"),
					DSL.foreignKey("parent_lemma").references("lemma"),*/,
					DSL.foreignKey("parent").references("lemma_node")
				)
				.execute();
			/*this.context.createIndex().on("lemma_node", "lemma").execute();
			this.context.createIndex().on("lemma_node", "parent").execute();
			this.context.createIndex().on("lemma_node", "parent_lemma").execute();
			this.context.createIndex().on("lemma_node", "lemma").execute();
			this.context.createIndex().on("lemma_node", "children_lemmas").execute();
			this.context.createIndex().on("lemma_node", "lemma", "children_lemmas").execute();*/
			this.context.createIndex().on("lemma_node", "lemma", "parent_lemma").execute();
			
			this.context.createTable("lemma_node_pld")//to instance/child
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("lemma_node", SQLDataType.INTEGER)
				.column("pld", SQLDataType.INTEGER)
				.constraints(
					DSL.unique("lemma_node", "pld"),
					DSL.foreignKey("lemma_node").references("lemma_node")/*,
					DSL.foreignKey("pld").references("pld")*/
				)
				.execute();
			this.context.createIndex().on("lemma_node_pld", "lemma_node").execute();
			
			this.context.createTable("term_node")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("term", SQLDataType.INTEGER)
				.column("lemma", SQLDataType.INTEGER)
				.column("parent", SQLDataType.INTEGER)
				.column("parent_term", SQLDataType.INTEGER)
				.column("children_terms", SQLDataType.VARCHAR)
				.column("child_count", SQLDataType.INTEGER)
				.column("count", SQLDataType.INTEGER)
				.column("pld_count", SQLDataType.INTEGER)
				.constraints(
						DSL.unique("term", "parent", "children_terms"),
						DSL.foreignKey("lemma").references("lemma_node")/*
						DSL.foreignKey("term").references("term"),
						DSL.foreignKey("parent_term").references("term"),*/,
						DSL.foreignKey("parent").references("term_node")
				)
				.execute();
			//this.context.createIndex().on("term_node", "term").execute();
			//this.context.createIndex().on("term_node", "lemma").execute();
			//this.context.createIndex().on("term_node", "parent").execute();
			//this.context.createIndex().on("term_node", "parent_term").execute();
			//this.context.createIndex().on("term_node", "term", "parent_term").execute();
			//this.context.createIndex().on("term_node", "children_terms").execute();
			//this.context.createIndex().on("term_node", "term", "children_terms").execute();
			this.context.createIndex().on("term_node", "term", "parent_term").execute();
			
			this.context.createTable("term_node_pld")//to instance/child
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("term_node", SQLDataType.INTEGER)
				.column("pld", SQLDataType.INTEGER)
				.constraints(
					DSL.unique("term_node", "pld"),
					DSL.foreignKey("term_node").references("term_node")/*,
					DSL.foreignKey("pld").references("pld")*/
				)
				.execute();
			this.context.createIndex().on("term_node_pld", "term_node").execute();
			
			this.context.createTable("matching_candidate")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("matching", SQLDataType.INTEGER)
				.column("instance_count", SQLDataType.INTEGER)
				.column("class_count", SQLDataType.INTEGER)
				.column("done", SQLDataType.BOOLEAN.default_(false))
				.constraints(
					DSL.unique("matching")/*,
					DSL.foreignKey("matching").references("matching"),*/
				)
				.execute();
			this.context.createIndex().on("matching_candidate", "instance_count").execute();
			this.context.createIndex().on("matching_candidate", "class_count").execute();
			this.context.createIndex().on("matching_candidate", "instance_count", "class_count").execute();
			this.context.createIndex().on("matching_candidate", "done").execute();
			this.context.createIndex().on("matching_candidate", "instance_count", "done").execute();
			this.context.createIndex().on("matching_candidate", "class_count", "done").execute();
			this.context.createIndex().on("matching_candidate", "instance_count", "class_count", "done").execute();
			
			this.context.createTable("matching_connection")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("matching", SQLDataType.INTEGER)
				.column("lemma_node", SQLDataType.INTEGER)//instance
				.column("term_node", SQLDataType.INTEGER)//instance
				.constraints(
					DSL.unique("matching", "lemma_node", "term_node"),
					/*DSL.foreignKey("matching").references("matching"),*/
					DSL.foreignKey("lemma_node").references("lemma_node"),
					DSL.foreignKey("term_node").references("term_node")
				)
				.execute();
			/*this.context.createIndex().on("matching_connection", "matching").execute();
			this.context.createIndex().on("matching_connection", "lemma_node").execute();
			this.context.createIndex().on("matching_connection", "term_node").execute();
			this.context.createIndex().on("matching_connection", "lemma_node, matching").execute();
			this.context.createIndex().on("matching_connection", "term_node, matching").execute();*/
		}
	
		
		@Override
		protected void prepare() {
			try {
				super.connection.setAutoCommit(false);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

}
