package evaluation;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import db.DbConnection;
import db.PairType;
import utils.DbUtils;

public class StatisticsCalculator {
	
	public static Map<String, String> calcCrawlDbStats(File dbFile) throws SQLException {
		Map<String, String> stats = new LinkedHashMap<String, String>();
		
		double dbSizeInMib = dbFile.length() / 1024 / 1024 / 1024D;
		stats.put("DBS_SIZE", String.format("%.2f", dbSizeInMib));
		System.out.print(".");
		
		DbConnection db = new DbConnection(dbFile, true) {};
		
		long crawls = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM crawl")).getLong(1);
		stats.put("CRAWLS", String.valueOf(crawls));
		System.out.print(".");
		
		long files = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM file")).getLong(1);
		stats.put("FILES", String.valueOf(files));
		System.out.print(".");
		
		long plds = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM pld")).getLong(1);
		stats.put("PLDS", String.valueOf(plds));
		System.out.print(".");
		
		long pages = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM page")).getLong(1);
		stats.put("PAGES", String.valueOf(pages));
		System.out.print(".");
		
		double pages_per_pld = (double) pages / plds;
		stats.put("PAGES_PER_PLD", String.valueOf(pages_per_pld));
		System.out.print(".");
		
		long sentences = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM sentence")).getLong(1);
		stats.put("SENTENCES", String.valueOf(sentences));
		System.out.print(".");
		
		double page_sentences = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM page_sentence")).getLong(1);
		double sentenece_per_pages = (double) page_sentences / pages;
		stats.put("SENTENCES_PER_PAGE", String.valueOf(sentenece_per_pages));
		double pages_per_sentence = (double) page_sentences / sentences;
		stats.put("PAGES_PER_SENTENCE", String.valueOf(pages_per_sentence));
		System.out.print(".");
		
		double sentences_per_pld = (double) page_sentences / plds;
		stats.put("SENTENCES_PER_PLD", String.valueOf(sentences_per_pld));
		System.out.print(".");
		
		long matches = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM matching")).getLong(1);
		stats.put("MATCHES", String.valueOf(matches));
		System.out.print(".");
		
		long instances = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM instance")).getLong(1);
		stats.put("INSTANCES", String.valueOf(instances));
		System.out.print(".");
		
		long classes = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM class")).getLong(1);
		stats.put("CLASSES", String.valueOf(classes));
		System.out.print(".");
		
		double instances_per_match = (double) instances / matches;
		stats.put("INSTANCES_PER_MATCH", String.valueOf(instances_per_match));
		System.out.print(".");
		
		double classes_per_match = (double) classes / matches;
		stats.put("CLASSES_PER_MATCH", String.valueOf(classes_per_match));
		System.out.print(".");
		
		long lemmas = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM lemmagroup")).getLong(1);
		stats.put("LEMMAS", String.valueOf(lemmas));
		System.out.print(".");
		
		long terms = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM term")).getLong(1);
		stats.put("TERM", String.valueOf(terms));
		System.out.print(".");
		
		long premods = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM premod")).getLong(1);
		stats.put("PREMODIFIERS", String.valueOf(premods));
		System.out.print(".");
		
		long nounGroups = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM noungroup")).getLong(1);
		stats.put("NOUN_GROUPS", String.valueOf(nounGroups));
		System.out.print(".");
		
		long postmods = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM postmod")).getLong(1);
		stats.put("POSTMODIFIERS", String.valueOf(postmods));
		System.out.print(".");

		System.out.println();
		
		db.close();
		return stats;
	}
	
	public static Map<String, String> calcPatternStats(File dbFile) throws SQLException {
		Map<String, String> stats = new LinkedHashMap<String, String>();
		
		DbConnection db = new DbConnection(dbFile, true) {};
		Map<Integer, Pair<String, AtomicLong>> patternCount = new LinkedHashMap<Integer, Pair<String, AtomicLong>>();
		
		ResultSet patternSet = db.connection.createStatement().executeQuery("SELECT id, name FROM pattern");
		while (patternSet.next()) {
			patternCount.put(patternSet.getInt("id"), MutablePair.of(patternSet.getString("name"), new AtomicLong()));
		}
		
		ResultSet matchingSet = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT pattern FROM matching"));
		while (matchingSet.next()) {
			patternCount.get(matchingSet.getInt(1)).getRight().incrementAndGet();
		}
		db.close();
		
		for (Pair<String, AtomicLong> pattern: patternCount.values().stream().sorted((p1, p2) -> (int) Math.signum(p2.getRight().get() - p1.getRight().get())).collect(Collectors.toList())) {
			stats.put(pattern.getLeft(), String.valueOf(pattern.getRight()));
		}
		
		return stats;
	}
	
	public static Map<String, String> calcTupleDbStats(File tupleDbFile, File helperDbFile) throws SQLException {
		Map<String, String> stats = new LinkedHashMap<String, String>();
		
		double dbSizeInMib = tupleDbFile.length() / 1024 / 1024 / 1024D;
		stats.put("DBS_SIZE", String.format("%.2f", dbSizeInMib));
		System.out.print(".");
		
		DbConnection tupleDb = new DbConnection(tupleDbFile, true) {};
		DbConnection helperDb = new DbConnection(helperDbFile, true) {};
				
		long lemmaTuples = DbUtils.getFirstRow(tupleDb.connection.createStatement().executeQuery("SELECT COUNT(*) FROM lemma_pair")).getLong(1);
		stats.put("LEMMA_TUPLES", String.valueOf(lemmaTuples));
		System.out.print(".");
		
		ResultSet lemmaAverageCounts = DbUtils.getFirstRow(tupleDb.connection.createStatement().executeQuery("SELECT AVG(count), AVG(pld_count) FROM lemma_pair"));
		double lemmaCount = lemmaAverageCounts.getDouble(1);
		double lemmaPldCount = lemmaAverageCounts.getDouble(2);
		stats.put("LEMMA_COUNT", String.valueOf(lemmaCount));
		stats.put("LEMMA_PLD_COUNT", String.valueOf(lemmaPldCount));
		System.out.print(".");
		
		ResultSet lemmaNodeCount = DbUtils.getFirstRow(helperDb.connection.createStatement().executeQuery("SELECT COUNT(*), AVG(child_count) FROM lemma_node WHERE parent = -1"));
		long lemmaClassNodes = lemmaNodeCount.getLong(1);
		double lemmaInstancesPerClassNodes = lemmaNodeCount.getDouble(2);
		stats.put("LEMMA_COORDINATE_GROUPS", String.valueOf(lemmaClassNodes));
		stats.put("LEMMA_COORDINATES_PER_GROUP", String.valueOf(lemmaInstancesPerClassNodes));
		System.out.print(".");
		

		long termTuples = DbUtils.getFirstRow(tupleDb.connection.createStatement().executeQuery("SELECT COUNT(*) FROM term_pair")).getLong(1);
		stats.put("TERM_TUPLES", String.valueOf(termTuples));
		System.out.print(".");
		
		ResultSet termAverageCounts = DbUtils.getFirstRow(tupleDb.connection.createStatement().executeQuery("SELECT AVG(count), AVG(pld_count) FROM term_pair"));
		double termCount = termAverageCounts.getDouble(1);
		double termPldCount = termAverageCounts.getDouble(2);
		stats.put("TERM_COUNT", String.valueOf(termCount));
		stats.put("TERM_PLD_COUNT", String.valueOf(termPldCount));
		System.out.print(".");
		
		ResultSet termNodeCount = DbUtils.getFirstRow(helperDb.connection.createStatement().executeQuery("SELECT COUNT(*), AVG(child_count) FROM term_node WHERE parent = -1"));
		long termClassNodes = termNodeCount.getLong(1);
		double termInstancesPerClassNodes = termNodeCount.getDouble(2);
		stats.put("TERM_COORDINATE_GROUPS", String.valueOf(termClassNodes));
		stats.put("TERM_COORDINATES_PER_GROUP", String.valueOf(termInstancesPerClassNodes));
		System.out.print(".");

		System.out.println();
		
		tupleDb.close();
		helperDb.close();
		return stats;
	}
	
	public static List<Pair<Entry<String, String>, Entry<Integer, Integer>>> getTopTuples(File crawlDbFile, File tupleDbFile, PairType pairType, CountAttribute countAttribute, int limit) throws SQLException {
		List<Pair<Entry<String, String>, Entry<Integer, Integer>>> tuples = new ArrayList<Pair<Entry<String, String>, Entry<Integer, Integer>>>();
		
		DbConnection crawlDb = new DbConnection(crawlDbFile, true) {};
		DbConnection tupleDb = new DbConnection(tupleDbFile, true) {};
		
		PreparedStatement getTermQuery = crawlDb.connection.prepareStatement(
			pairType == PairType.LEMMA ?
			"SELECT words FROM lemmagroup WHERE id = ?" :
			"SELECT pre.words || \" \" || ng.words || \" \" || post.words FROM term as t, premod as pre, noungroup as ng, postmod as post WHERE t.id = ? AND pre.id = t.premod AND ng.id = t.noungroup AND post.id = t.postmod"
		);
		
		ResultSet tupleSet = tupleDb.connection.createStatement().executeQuery("SELECT instance, class, count, pld_count FROM " + pairType.name().toLowerCase() + "_pair ORDER BY " + countAttribute.column + " DESC LIMIT " + limit);
		
		while (tupleSet.next()) {
			getTermQuery.setInt(1, tupleSet.getInt(1));
			String instanceString = DbUtils.getFirstRow(getTermQuery.executeQuery()).getString(1).trim();
			
			getTermQuery.setInt(1, tupleSet.getInt(2));
			String classString = DbUtils.getFirstRow(getTermQuery.executeQuery()).getString(1).trim();
			
			tuples.add(Pair.of(Pair.of(instanceString, classString), Pair.of(tupleSet.getInt(3), tupleSet.getInt(4))));
		}
		
		crawlDb.close();
		tupleDb.close();
		return tuples;
	}
	
	public static Map<String, String> calcTaxonomyDbStats(File dbFile) throws SQLException {
		Map<String, String> stats = new LinkedHashMap<String, String>();
		
		double dbSizeInMib = dbFile.length() / 1024 / 1024 / 1024D;
		stats.put("DBS_SIZE", String.format("%.2f", dbSizeInMib));
		System.out.print(".");
		
		DbConnection db = new DbConnection(dbFile, true) {};
		
		long lemmaNodes = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM lemma_node")).getLong(1);
		stats.put("LEMMA_NODES", String.valueOf(lemmaNodes));
		System.out.print(".");
		
		ResultSet lemmaEdgeInfo = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*), COUNT(DISTINCT child), COUNT(DISTINCT parent) FROM lemma_edge"));
		long lemmaEdges = lemmaEdgeInfo.getLong(1);
		long lemmaChildren = lemmaEdgeInfo.getLong(2);
		long lemmaParents = lemmaEdgeInfo.getLong(3);
		double lemmaChildDegree = (double) lemmaEdges / lemmaParents;
		double lemmaParentDegree = (double) lemmaEdges / lemmaChildren;
		stats.put("LEMMA_EDGES", String.valueOf(lemmaEdges));
		stats.put("LEMMA_CHILDREN", String.valueOf(lemmaChildDegree));
		stats.put("LEMMA_PARENTS", String.valueOf(lemmaParentDegree));
		System.out.print(".");
		

		long termNodes = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*) FROM term_node")).getLong(1);
		stats.put("TERM_NODES", String.valueOf(termNodes));
		System.out.print(".");
		
		ResultSet termEdgeInfo = DbUtils.getFirstRow(db.connection.createStatement().executeQuery("SELECT COUNT(*), COUNT(DISTINCT child), COUNT(DISTINCT parent) FROM term_edge"));
		long termEdges = termEdgeInfo.getLong(1);
		long termChildren = termEdgeInfo.getLong(2);
		long termParents = termEdgeInfo.getLong(3);
		double termChildDegree = (double) termEdges / termParents;
		double termParentDegree = (double) termEdges / termChildren;
		stats.put("TERM_EDGES", String.valueOf(termEdges));
		stats.put("TERM_CHILDREN", String.valueOf(termChildDegree));
		stats.put("TERM_PARENTS", String.valueOf(termParentDegree));
		System.out.print(".");

		System.out.println();
		
		db.close();
		return stats;
	}

}
