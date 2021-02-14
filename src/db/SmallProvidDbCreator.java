package db;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import utils.DbUtils;

public class SmallProvidDbCreator {
	
	public static void createSmallProvidenceDb(File crawlDbFile, File outputDbFile) throws SQLException {
		DbConnection crawlDb = new DbConnection(crawlDbFile, true) {};
		DbConnection outputDb = new DbConnection(outputDbFile, false) {
			@Override
			protected void prepare() {
				try {
					this.connection.setAutoCommit(false);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			@Override
			protected void createTables() {
				this.context.createTable("sentence")
					.column("id", SQLDataType.INTEGER.identity(true))
					.column("sentence", SQLDataType.CLOB)
					.constraints(
						DSL.unique("sentence")
					)
				.execute();
				
				this.context.createTable("sentence_pld")
					.column("id", SQLDataType.INTEGER.identity(true))
					.column("sentence", SQLDataType.INTEGER)
					.column("pld", SQLDataType.VARCHAR)
					.constraints(
						DSL.unique("sentence", "pld"),
						DSL.foreignKey("sentence").references("sentence")
					)
				.execute();
				this.context.createIndex().on("sentence", "sentence").execute();
				this.context.createIndex().on("sentence", "pld").execute();
				
				this.context.createTable("lemma")
					.column("id", SQLDataType.INTEGER.identity(true))
					.column("lemma", SQLDataType.VARCHAR)
					.constraints(
						DSL.unique("lemma")
					)
				.execute();
				
				this.context.createTable("term")
					.column("id", SQLDataType.INTEGER.identity(true))
					.column("premod", SQLDataType.VARCHAR)
					.column("nouns", SQLDataType.VARCHAR)
					.column("postmod", SQLDataType.VARCHAR)
					.constraints(
						DSL.unique("premod", "nouns", "postmod")
					)
				.execute();
				
				this.context.createTable("matching")
					.column("id", SQLDataType.INTEGER.identity(true))
					.column("sentence", SQLDataType.INTEGER)
					.column("pattern", SQLDataType.VARCHAR)
					.constraints(
						DSL.unique("sentence", "pattern")
					)
				.execute();
				this.context.createIndex().on("matching", "sentence").execute();
				this.context.createIndex().on("matching", "pattern").execute();
			}
		};
		
		PreparedStatement insertSentenceQuery = crawlDb.connection.prepareStatement("INSERT INTO sentence (id, sentence) VALUES (?, ?)");
		ResultSet sentences = outputDb.connection.createStatement().executeQuery("SELECT id, sentence FROM sentence");
		while (sentences.next()) {
			insertSentenceQuery.setInt(1, sentences.getInt(1));
			insertSentenceQuery.setString(1, sentences.getString(2));
			insertSentenceQuery.execute();
		}
		outputDb.connection.commit();
		
		PreparedStatement insertSentencePldQuery = crawlDb.connection.prepareStatement("INSERT INTO sentence_pld (sentence, pld) VALUES (?, ?)");
		PreparedStatement getPldQuery = crawlDb.connection.prepareStatement("SELECT pld FROM pld WHERE id = ?");
		ResultSet sentencePlds = outputDb.connection.createStatement().executeQuery("SELECT sentence, pld FROM sentence_pld");
		while (sentencePlds.next()) {
			getPldQuery.setInt(1, sentencePlds.getInt(2));
			String pld = DbUtils.getFirstRow(getPldQuery.executeQuery()).getString(1);
			insertSentencePldQuery.setInt(1, sentencePlds.getInt(1));
			insertSentencePldQuery.setString(2, pld);
			insertSentencePldQuery.execute();
		}
		outputDb.connection.commit();
		
		PreparedStatement insertLemmaQuery = crawlDb.connection.prepareStatement("INSERT INTO lemma (id, lemma) VALUES (?, ?)");
		ResultSet lemmas = outputDb.connection.createStatement().executeQuery("SELECT id, words FROM lemmagroup");
		while (lemmas.next()) {
			insertLemmaQuery.setInt(1, lemmas.getInt(1));
			insertLemmaQuery.setString(2, lemmas.getString(2));
			insertLemmaQuery.execute();
		}
		outputDb.connection.commit();
		
		PreparedStatement insertTermQuery = crawlDb.connection.prepareStatement("INSERT INTO term (id, premod, nouns, postmod) VALUES (?, ?, ?, ?)");
		PreparedStatement getTermQuery = crawlDb.connection.prepareStatement("SELECT pre.words, ng.words, post.words FROM premod as pre, noungroup as ng, postmod as post WHERE pre.id = ? AND ng.id = ? AND post.id = ?");
		ResultSet terms = outputDb.connection.createStatement().executeQuery("SELECT id, premod, noungroup, postmod FROM term");
		while (terms.next()) {
			getTermQuery.setInt(1, terms.getInt(2));
			getTermQuery.setInt(2, terms.getInt(3));
			getTermQuery.setInt(3, terms.getInt(4));
			ResultSet term = DbUtils.getFirstRow(getTermQuery.executeQuery());
			insertTermQuery.setInt(1, sentencePlds.getInt(1));
			insertTermQuery.setString(2, term.getString(1));
			insertTermQuery.setString(3, term.getString(2));
			insertTermQuery.setString(4, term.getString(3));
			insertTermQuery.execute();
		}
		outputDb.connection.commit();

		Map<Integer, String> patternMap = new HashMap<Integer, String>();
		PreparedStatement insertMatchingQuery = crawlDb.connection.prepareStatement("INSERT INTO matching (id, sentence, pattern) VALUES (?, ?, ?)");
		PreparedStatement getPatternQuery = crawlDb.connection.prepareStatement("SELECT name FROM pattern WHERE id = ?");
		ResultSet matchings = outputDb.connection.createStatement().executeQuery("SELECT id, sentence, pattern FROM matching");
		while (matchings.next()) {
			int patternId = matchings.getInt(3);
			String patternName = patternMap.get(patternId);
			if (patternName == null) {
				getPatternQuery.setInt(1, patternId);
				patternName = DbUtils.getFirstRow(getPatternQuery.executeQuery()).getString(1);
				patternMap.put(patternId, patternName);
			}
			insertMatchingQuery.setInt(1, matchings.getInt(1));
			insertMatchingQuery.setInt(2, matchings.getInt(2));
			insertMatchingQuery.setString(3, patternName);
			insertMatchingQuery.execute();
		}
		outputDb.connection.commit();
		
		crawlDb.close();
		outputDb.close();
	}
	
	public static void createJoinedPairDb(File crawlDbFile, File tupleDbFile, File outputDbFile, PairType pairType) throws SQLException {
		DbConnection crawlDb = new DbConnection(crawlDbFile, true) {};
		DbConnection tupleDb = new DbConnection(tupleDbFile, true) {};
		DbConnection outputDb = new DbConnection(outputDbFile, false) {
			@Override
			protected void prepare() {
				try {
					this.connection.setAutoCommit(false);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			@Override
			protected void createTables() {
				if (pairType == PairType.LEMMA) {
					this.context.createTable("tuple")
						.column("id", SQLDataType.INTEGER.identity(true))
						.column("instance", SQLDataType.VARCHAR)
						.column("class", SQLDataType.VARCHAR)
						.column("count", SQLDataType.INTEGER)
						.column("pld_count", SQLDataType.INTEGER)
						.column("tuple_id", SQLDataType.INTEGER)
						.constraints(
							DSL.unique("instance", "class")
						)
					.execute();
				} else {					
					this.context.createTable("tuple")
						.column("id", SQLDataType.INTEGER.identity(true))
						.column("instance_premod", SQLDataType.VARCHAR)
						.column("instance_nouns", SQLDataType.VARCHAR)
						.column("instance_postmod", SQLDataType.VARCHAR)
						.column("class_premod", SQLDataType.VARCHAR)
						.column("class_nouns", SQLDataType.VARCHAR)
						.column("class_postmod", SQLDataType.VARCHAR)
						.column("count", SQLDataType.INTEGER)
						.column("pld_count", SQLDataType.INTEGER)
						.column("tuple_id", SQLDataType.INTEGER)
						.constraints(
							DSL.unique("instance_premod", "instance_nouns", "instance_nouns", "class_premod", "class_nouns", "class_postmod")
						)
					.execute();
				}
				this.context.createTable("tuple_pld")
					.column("id", SQLDataType.INTEGER.identity(true))
					.column("pld", SQLDataType.VARCHAR)
					.column("tuple", SQLDataType.INTEGER)
					.constraints(
						DSL.unique("pld", "tuple"),
						DSL.foreignKey("tuple").references("tuple")
					)
				.execute();
			}
		};
		int tupleCount = DbUtils.getFirstRow(tupleDb.connection.createStatement().executeQuery("SELECT count(*) FROM " + pairType.name().toLowerCase() + "_pair")).getInt(1);
		PreparedStatement getTermQuery = crawlDb.connection.prepareStatement(
			pairType == PairType.LEMMA ?
			"SELECT words FROM lemmagroup WHERE id = ?" :
			"SELECT pre.words, ng.words, post.words FROM term as t, premod as pre, noungroup as ng, postmod as post WHERE t.id = ? AND pre.id = t.premod AND ng.id = t.noungroup AND post.id = t.postmod"
		);
		PreparedStatement insertTupleQuery = outputDb.connection.prepareStatement(
			pairType == PairType.LEMMA ?
			"INSERT INTO tuple (instance, class, count, pld_count, tuple_id) VALUES (?, ?, ?, ?, ?)":
			"INSERT INTO tuple (instance_premod, instance_nouns, instance_postmod, class_premod, class_nouns, class_postmod, count, pld_count, tuple_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
		);
		PreparedStatement getPldsQuery = tupleDb.connection.prepareStatement("SELECT pld FROM " + pairType.name().toLowerCase()+ "_pair_pld WHERE " + pairType.name().toLowerCase() + "_pair = ?");
		PreparedStatement getPldQuery = crawlDb.connection.prepareStatement("SELECT pld FROM pld WHERE id = ?");
		PreparedStatement insertPldQuery = outputDb.connection.prepareStatement("INSERT INTO tuple_pld (pld, tuple) VALUES (?, ?)");
		ResultSet tuples = tupleDb.connection.createStatement().executeQuery("SELECT id, instance, class, count, pld_count FROM " + pairType.name().toLowerCase() + "_pair");
		int i = 0;
		while (tuples.next()) {
			if (i % 1000000 == 0) {
				System.out.println(String.format("tuples %.2f %%", 100.0D * i / tupleCount));
			}
			i++;
			int tupleId = tuples.getInt(1);
			getTermQuery.setInt(1, tuples.getInt(2));
			ResultSet instanceTerm = DbUtils.getFirstRow(getTermQuery.executeQuery());
			getTermQuery.setInt(1, tuples.getInt(3));
			ResultSet classTerm = DbUtils.getFirstRow(getTermQuery.executeQuery());
			if (pairType == PairType.LEMMA) {
				insertTupleQuery.setString(1, instanceTerm.getString(1));
				insertTupleQuery.setString(2, classTerm.getString(1));
				insertTupleQuery.setInt(3, tuples.getInt(4));
				insertTupleQuery.setInt(4, tuples.getInt(5));
				insertTupleQuery.setInt(5, tupleId);
			} else {
				insertTupleQuery.setString(1, instanceTerm.getString(1));
				insertTupleQuery.setString(2, instanceTerm.getString(2));
				insertTupleQuery.setString(3, instanceTerm.getString(3));
				insertTupleQuery.setString(4, classTerm.getString(1));
				insertTupleQuery.setString(5, classTerm.getString(2));
				insertTupleQuery.setString(6, classTerm.getString(3));
				insertTupleQuery.setInt(7, tuples.getInt(4));
				insertTupleQuery.setInt(8, tuples.getInt(5));
				insertTupleQuery.setInt(9, tupleId);
			}
			insertTupleQuery.execute();
			getPldsQuery.setInt(1, tupleId);
			ResultSet plds = getPldsQuery.executeQuery();
			while (plds.next()) {
				getPldQuery.setInt(1, plds.getInt(1));
				String pld = DbUtils.getFirstRow(getPldQuery.executeQuery()).getString(1);
				insertPldQuery.setString(1, pld);
				insertPldQuery.setInt(2, tupleId);
				insertPldQuery.execute();
			}
			if (i % 1000000 == 0) {
				outputDb.connection.commit();
			}
		}
		outputDb.connection.commit();
		crawlDb.close();
		tupleDb.close();
		outputDb.close();
	}
	
}
