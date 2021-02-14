package linking;

import java.io.File;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import db.DbConnection;
import db.PairType;
import evaluation.CountAttribute;
import evaluation.PairTestAnswer;
import utils.DbUtils;

public class LinkCheck {
	
	public enum TestType {
		PER_PATTERN, WHOLE
	}
	
	private final static int MAX_RANDOM_PICKS = 100;
	
	private static double getTrueRate(Stream<PairTestAnswer> values) {
		Pair<AtomicInteger, AtomicInteger> fraction = values.filter(a -> a.truth.level != 0).map(a -> a.truth.isTrue())
			.reduce(Pair.of(new AtomicInteger(), new AtomicInteger()), (f, b) -> {
				if (b) {
					f.getLeft().addAndGet(1);
				}
				f.getRight().addAndGet(1);
				return f;
			}, (f1, f2) -> {
				f1.getLeft().addAndGet(f2.getLeft().get());
				f1.getRight().addAndGet(f2.getRight().get());
				return f1;
			});
		return (double) fraction.getLeft().get() / fraction.getRight().get();
	}
	
	public static void evaluate(File pairDbFile, File sourceDbFile, File resultsDbFile, TestType testType, PairType pairType, CountAttribute countAttribute, int minSupport, int n, boolean forceRandomPicks) throws SQLException {
		DbConnection sourceDb = new DbConnection(sourceDbFile, true) {};
		DbConnection pairDb = new DbConnection(pairDbFile, true) {};
		TestDbConnection testDb = new TestDbConnection(resultsDbFile);
		
		double testTruth;
		
		List<Pair<Integer, PairTestAnswer>> results = new ArrayList<Pair<Integer, PairTestAnswer>>();
		List<Pair<Integer, Pair<String, String>>> pairs = samplePairs(sourceDb, pairDb, pairType, countAttribute, minSupport, n, forceRandomPicks);
//		for (int i = 0; i < pairs.size(); i++) {
//			PairTestAnswer answer = askPair(i + 1, pairs.get(i).getRight().getLeft(), pairs.get(i).getRight().getRight());
//			if (answer == null) {
//				break;
//			}
//			results.add(Pair.of(pairs.get(i).getLeft(), answer));
//		}
		testTruth = getTrueRate(results.stream().map(r -> r.getRight()));
		int testId = insertTest(testDb, testType, pairType, countAttribute, minSupport, n, testTruth);
		insertAnswers(testDb, testId, PairTestAnswer.ANSWERS.values());
		for (Pair<Integer, PairTestAnswer> answer: results) {
			insertPair(testDb, testId, answer.getLeft(), null, answer.getRight().no);
		}
		
		System.out.println();		
		System.out.println(String.format("Accuracy is %.2f%%", testTruth * 100));
		
		sourceDb.close();
		pairDb.close();
		testDb.close();
	}
	
	private static int insertTest(DbConnection resultDb, TestType testType, PairType pairType, CountAttribute countAttribute, int minSupport, int n, double truth) throws SQLException {
		PreparedStatement insertQuery = resultDb.connection.prepareStatement("INSERT INTO test (test_type, pair_type, count_attribute, min_support, n, truth) VALUES (?, ?, ?, ?, ?, ?)");
		insertQuery.setString(1, testType.name());
		insertQuery.setString(2, pairType.name());
		insertQuery.setString(3, countAttribute.name());
		insertQuery.setInt(4, minSupport);
		insertQuery.setInt(5, n);
		insertQuery.setDouble(6, truth);
		insertQuery.execute();
		insertQuery.close();
		return DbUtils.getInsertedKey(insertQuery);
	}
	
	private static Map<Integer, Integer> insertAnswers(DbConnection resultDb, int testId, Collection<PairTestAnswer> answers) throws SQLException {
		Map<Integer, Integer> answerMap = new HashMap<Integer, Integer>();
		PreparedStatement insertQuery = resultDb.connection.prepareStatement("INSERT INTO answer (test, no, text, truth, true) VALUES (?, ?, ?, ?, ?)");
		for (PairTestAnswer answer: answers) {
			insertQuery.setInt(1, testId);
			insertQuery.setInt(2, answer.no);
			insertQuery.setString(3, answer.text);
			insertQuery.setInt(4, answer.truth.level);
			insertQuery.setBoolean(5, answer.truth.isTrue());
			insertQuery.execute();
			answerMap.put(answer.no, DbUtils.getInsertedKey(insertQuery));
		}
		insertQuery.close();
		return answerMap;
	}
	
	private static void insertPatternResult(DbConnection resultDb, int testId, String pattern, double truth) throws SQLException {
		PreparedStatement insertQuery = resultDb.connection.prepareStatement("INSERT INTO pattern_result (test, pattern, truth) VALUES (?, ?, ?)");
		insertQuery.setInt(1, testId);
		insertQuery.setString(2, pattern);
		insertQuery.setDouble(3, truth);
		insertQuery.execute();
		insertQuery.close();
	}
	
	private static void insertPair(DbConnection resultDb, int testId, int pairId, String pattern, int answer) throws SQLException {
		PreparedStatement insertQuery = resultDb.connection.prepareStatement("INSERT INTO pair (test, pair_id, pattern, answer) VALUES (?, ?, ?, ?)");
		insertQuery.setInt(1, testId);
		insertQuery.setInt(2, pairId);
		insertQuery.setString(3, pattern);
		insertQuery.setInt(4, answer);
		insertQuery.execute();
		insertQuery.close();
	}
	
	private static List<Pair<Integer, String>> loadPatterns(DbConnection sourceDb) throws SQLException {
		List<Pair<Integer, String>> patterns = new ArrayList<Pair<Integer, String>>();
		ResultSet patternSet = sourceDb.connection.createStatement().executeQuery("SELECT id, name FROM pattern");
		while (patternSet.next()) {
			patterns.add(Pair.of(patternSet.getInt("id"), patternSet.getString("name")));
		}
		return patterns;
	}
	
	private static List<Integer> getPairIdsWithSupport(DbConnection pairDb, PairType pairType, CountAttribute countAttribute, int minSupport) throws SQLException {
		List<Integer> ids = new ArrayList<Integer>();
		ResultSet pairSet = pairDb.connection.createStatement().executeQuery("SELECT id FROM " + pairType.name().toLowerCase() + "_pair WHERE " + countAttribute.column + " >= " + minSupport);
		while (pairSet.next()) {
			ids.add(pairSet.getInt(1));
		}
		return ids;
	}
	
	private static List<Pair<String, List<Pair<Integer, Pair<String, String>>>>> samplePairsByPattern(DbConnection sourceDb, DbConnection pairDb, PairType pairType, CountAttribute countAttribute, int minSupport, int n, boolean forceRandomPicks) throws SQLException {
		List<Pair<Integer, String>> patterns = loadPatterns(sourceDb);
		List<Pair<String, List<Pair<Integer, Pair<String, String>>>>> patternPairs = new ArrayList<Pair<String, List<Pair<Integer, Pair<String, String>>>>>();
		
		if (minSupport <= 1 || forceRandomPicks) {
			Set<Integer> usedMatchings = new HashSet<Integer>();
			Set<Integer> usedPairs = new HashSet<Integer>();
			for (Pair<Integer, String> pattern: patterns) {
				List<Pair<Integer, Pair<String, String>>> pairs = new ArrayList<Pair<Integer, Pair<String, String>>>();
				patternPairs.add(Pair.of(pattern.getRight(), pairs));
				
				List<Integer> matchingIds = new ArrayList<Integer>();
				ResultSet pairSet = sourceDb.connection.createStatement().executeQuery("SELECT id FROM matching WHERE pattern = " + pattern.getLeft());
				while (pairSet.next()) {
					matchingIds.add(pairSet.getInt(1));
				}
				outer:
				for (int i = 0; i < n; i++) {
					for (int t = 0; t < MAX_RANDOM_PICKS; t++) {
						if (i == matchingIds.size()) {
							break outer;
						}
						int matchingId;
						while (true) {
							int idPos = (int) (Math.random() * (matchingIds.size() - i));
							matchingId = matchingIds.get(idPos);
							matchingIds.set(idPos, matchingIds.get(matchingIds.size() - 1 - i));
							//if (usedMatchings.add(matchingId)) {
							break;
						}
						ResultSet pair = DbUtils.getFirstRow(pairDb.connection.createStatement().executeQuery("SELECT id, instance, class, " + countAttribute.column + " FROM " + pairType.name().toLowerCase() + "_pair WHERE id = (SELECT " + pairType.name().toLowerCase() + "_pair FROM matching_connection WHERE matching = " + matchingId + " ORDER BY RANDOM() LIMIT 1)"));
						if (!pair.isClosed() && usedPairs.add(pair.getInt(1)) && pair.getInt(4) >= minSupport) {
							pairs.add(Pair.of(pair.getInt(1), getPairStrings(sourceDb, pairType, pair.getInt(2), pair.getInt(3))));
							break;
						}
					}
				}
			}
		} else {
			Set<Integer> usedPairs = new HashSet<Integer>();
			for (Pair<Integer, String> pattern: patterns) {
				List<Pair<Integer, Pair<String, String>>> pairs = new ArrayList<Pair<Integer, Pair<String, String>>>();
				patternPairs.add(Pair.of(pattern.getRight(), pairs));
				
				List<Integer> pairIds = new ArrayList<Integer>();
				ResultSet matchingSet = sourceDb.connection.createStatement().executeQuery("SELECT id FROM matching WHERE pattern = " + pattern.getLeft());
				while (matchingSet.next()) {
					ResultSet pairSet = pairDb.connection.createStatement().executeQuery("SELECT p.id FROM matching_connection as mc, " + pairType.name().toLowerCase() + "_pair as p WHERE mc.matching = " + matchingSet.getInt(1) + " AND p.id = mc." + pairType.name().toLowerCase() + "_pair AND p." + countAttribute.column + " >= " + minSupport);
					while (pairSet.next()) {						
						pairIds.add(pairSet.getInt(1));
					}
				}
				outer:
				for (int i = 0; i < n; i++) {
					for (int t = 0; t < MAX_RANDOM_PICKS; t++) {
						if (i == pairIds.size()) {
							break outer;
						}
						int pairId;
						while (true) {
							int idPos = (int) (Math.random() * (pairIds.size() - i));
							pairId = pairIds.get(idPos);
							pairIds.set(idPos, pairIds.get(pairIds.size() - 1 - i));
							//if (usedPairs.add(pairId)) {
							break;
						}
						ResultSet pair = DbUtils.getFirstRow(pairDb.connection.createStatement().executeQuery("SELECT id, instance, class FROM " + pairType.name().toLowerCase() + "_pair WHERE id = " + pairId));
						if (!pair.isClosed()) {
							pairs.add(Pair.of(pair.getInt("id"), getPairStrings(sourceDb, pairType, pair.getInt("instance"), pair.getInt("class"))));
							break;
						}
					}
				}
			}
		}
		return patternPairs;
	}
	
	private static List<Pair<Integer, Pair<String, String>>> samplePairs(DbConnection sourceDb, DbConnection pairDb, PairType pairType, CountAttribute countAttribute, int minSupport, int n, boolean forceRandomPicks) throws SQLException {
		if (DbUtils.getFirstRow(pairDb.connection.createStatement().executeQuery("SELECT COUNT(*) FROM (SELECT id FROM " + pairType.name().toLowerCase() + "_pair WHERE " + countAttribute.column + " >= " + minSupport + " LIMIT " + n + ")")).getInt(1) < n) {
			return null;
		}
		
		List<Pair<Integer, Pair<String, String>>> pairs = new ArrayList<Pair<Integer, Pair<String, String>>>();
		
		if (minSupport <= 1 || forceRandomPicks) {
			Set<Integer> usedPairs = new HashSet<Integer>();
			for (int i = 0; i < n; i++) {
				pairs.add(samplePairByRandomPick(sourceDb, pairDb, pairType, countAttribute, minSupport, n, usedPairs));
			}
		} else {
			List<Integer> ids = getPairIdsWithSupport(pairDb, pairType, countAttribute, minSupport);
			for (int i = 0; i < n; i++) {
				for (int t = 0; t < MAX_RANDOM_PICKS; t++) {
					int idPos = (int) (Math.random() * (ids.size() - i));
					int id = ids.get(idPos);
					ids.set(idPos, ids.get(ids.size() - 1 - i));
					ResultSet pair = DbUtils.getFirstRow(pairDb.connection.createStatement().executeQuery("SELECT id, instance, class FROM " + pairType.name().toLowerCase() + "_pair WHERE id = " + id));
					if (!pair.isClosed()) {						
						pairs.add(Pair.of(pair.getInt("id"), getPairStrings(sourceDb, pairType, pair.getInt("instance"), pair.getInt("class"))));
						break;
					}
				}
			}
		}
		return pairs;
	}
	
	private static Pair<Integer, Pair<String, String>> samplePairByRandomPick(DbConnection sourceDb, DbConnection pairDb, PairType pairType, CountAttribute countAttribute, int minSupport, int n, Set<Integer> usedPairs) throws SQLException {
		int maxId = DbUtils.getFirstRow(pairDb.connection.createStatement().executeQuery("SELECT seq FROM sqlite_sequence where name='" + pairType.name().toLowerCase() + "_pair'")).getInt(1);
		
		outer:
		for (int i = 0; i < MAX_RANDOM_PICKS; i++) {
			int id;
			while (true) {
				id = (int) (Math.random() * maxId) + 1;
				if (usedPairs.add(id)) {
					break;
				} else {
					continue outer;
				}
			}
			ResultSet pair = pairDb.connection.createStatement().executeQuery("SELECT id, instance, class, " + countAttribute.column + " FROM " + pairType.name().toLowerCase() + "_pair WHERE id = " + id);
			if (!pair.next() || pair.getInt(4) < minSupport) {
				continue;
			}
			return Pair.of(pair.getInt(1), getPairStrings(sourceDb, pairType, pair.getInt(2), pair.getInt(3)));
		}
		return null;
	}
	
	private static Pair<String, String> getPairStrings(DbConnection sourceDb, PairType pairType, int instanceId, int classId) throws SQLException {
		String instanceString, classString;
		if (pairType == PairType.LEMMA) {
			instanceString = DbUtils.getFirstRow(sourceDb.connection.createStatement().executeQuery("SELECT words FROM lemmagroup WHERE id = " + instanceId)).getString(1);
			classString = DbUtils.getFirstRow(sourceDb.connection.createStatement().executeQuery("SELECT words FROM lemmagroup WHERE id = " + classId)).getString(1);
		} else {
			instanceString = DbUtils.getFirstRow(sourceDb.connection.createStatement()
				.executeQuery("SELECT pre.words || ' ' || ng.words || ' ' || post.words FROM term as t, premod as pre, noungroup as ng, postmod as post WHERE t.id = " + instanceId + " AND pre.id = t.premod AND ng.id = noungroup AND post.id = postmod"))
				.getString(1);
			classString = DbUtils.getFirstRow(sourceDb.connection.createStatement()
				.executeQuery("SELECT pre.words || ' ' || ng.words || ' ' || post.words FROM term as t, premod as pre, noungroup as ng, postmod as post WHERE t.id = " + classId + " AND pre.id = t.premod AND ng.id = noungroup AND post.id = postmod"))
				.getString(1);
		}
		return Pair.of(instanceString, classString);
	}
	
	private static class TestDbConnection extends DbConnection {

		public TestDbConnection(File dbFile) {
			super(dbFile);
		}
		
		@Override
		protected void createTables() {
			this.context.createTable("test")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("test_type", SQLDataType.VARCHAR)
				.column("pair_type", SQLDataType.VARCHAR)
				.column("count_attribute", SQLDataType.VARCHAR)
				.column("min_support", SQLDataType.INTEGER)
				.column("n", SQLDataType.INTEGER)
				.column("date", SQLDataType.DATE.default_(Date.valueOf(LocalDate.now())))
				.column("truth", SQLDataType.DOUBLE)
				.execute();
			
			this.context.createTable("answer")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("test", SQLDataType.INTEGER)
				.column("no", SQLDataType.INTEGER)
				.column("text", SQLDataType.VARCHAR)
				.column("truth", SQLDataType.INTEGER)
				.column("true", SQLDataType.BOOLEAN)
				.constraints(
					DSL.unique("test", "no"),
					DSL.foreignKey("test").references("test")
				)
				.execute();
			this.context.createIndex().on("answer", "test").execute();
			
			this.context.createTable("pattern_result")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("test", SQLDataType.INTEGER)
				.column("pattern", SQLDataType.VARCHAR)
				.column("truth", SQLDataType.DOUBLE)
				.constraints(
					DSL.unique("test", "pattern"),
					DSL.foreignKey("test").references("test")
				)
				.execute();
			this.context.createIndex().on("pattern_result", "test").execute();
			this.context.createIndex().on("pattern_result", "pattern").execute();
			
			this.context.createTable("pair")
				.column("id", SQLDataType.INTEGER.identity(true))
				.column("test", SQLDataType.INTEGER)
				.column("pattern", SQLDataType.INTEGER)
				.column("pair_id", SQLDataType.INTEGER)
				.column("answer", SQLDataType.INTEGER)
				.constraints(
					DSL.unique("test", "pattern", "pair_id"),
					DSL.foreignKey("test").references("test"),
					DSL.foreignKey("test", "answer").references("answer", "test", "no")
				)
				.execute();
			this.context.createIndex().on("pair", "test").execute();
		}
		
	}
	
}
