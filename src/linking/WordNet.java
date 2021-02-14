package linking;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.google.common.collect.Sets;

import db.DbConnection;
import net.sf.extjwnl.JWNLException;
import net.sf.extjwnl.data.IndexWord;
import net.sf.extjwnl.data.POS;
import net.sf.extjwnl.data.Pointer;
import net.sf.extjwnl.data.PointerType;
import net.sf.extjwnl.data.Synset;
import net.sf.extjwnl.data.Word;
import net.sf.extjwnl.dictionary.Dictionary;
import utils.DbUtils;
import utils.Utils;
import utils.measures.EnumCounter;

public class WordNet {
	
	public static void check() {
		try {
			Dictionary d = Dictionary.getDefaultResourceInstance();
			EnumCounter<PointerType> connectionCounter = new EnumCounter<PointerType>(PointerType.class);
			Set<String> indexWords = new HashSet<String>();
			Set<String> lemmas = new HashSet<String>();
			d.getIndexWordIterator(POS.NOUN).forEachRemaining(iw -> indexWords.add(iw.getLemma()));
			d.getSynsetIterator(POS.NOUN).forEachRemaining(s -> {
				s.getWords().forEach(w -> lemmas.add(w.getLemma()));
				s.getPointers().forEach(p -> connectionCounter.increase(p.getType()));
			});
			System.out.println(Utils.mapFormat(connectionCounter.getPrintableMap("", ""), true));
			System.out.println();
			System.out.println("indexWords:\t" + indexWords.size());
			System.out.println("lemmas:\t\t" + lemmas.size());
			System.out.println("iw-lemmas:\t" + Sets.difference(indexWords, lemmas).size());
			System.out.println("lemmas-iw:\t" + Sets.difference(lemmas, indexWords).size());
			System.out.println(new ArrayList<String>(Sets.difference(indexWords, lemmas)).subList(0, 10));
			System.out.println(new ArrayList<String>(Sets.difference(lemmas, indexWords)).subList(0, 10));
		} catch (JWNLException e) {
			e.printStackTrace();
		}
	}
	
	public static void writeDb(File dbFile) {
		WordNetDb db = new WordNetDb(dbFile, false);
		try {
			Dictionary d = Dictionary.getDefaultResourceInstance();
			for (Iterator<IndexWord> i = d.getIndexWordIterator(POS.NOUN); i.hasNext();) {
				IndexWord w = i.next();
				db.createIndexWord(w);
				for (Synset s: w.getSenses()) {
					db.createIndexWordSynsetConnection(w, s);
				}
			}
			for (Iterator<Synset> i = d.getSynsetIterator(POS.NOUN); i.hasNext();) {
				Synset s = i.next();
				db.createSynset(s);
				for (Word w: s.getWords()) {
					db.createWord(w);
					for (Pointer p: s.getPointers()) {
						for (Word pw: p.getTarget().getSynset().getWords()) {
							switch(p.getType()) {
							case HYPERNYM:
								db.createTuple(w, pw, false);
								break;
							case INSTANCE_HYPERNYM:
								db.createTuple(w, pw, true);
							default:
								break;
							}
						}
					}
				}
			}
			db.calcTransitiveClosure();
		} catch (JWNLException | SQLException e) {
			e.printStackTrace();
		}
		db.close();
	}
	
	public static void main(String[] args) {
		check();
		System.out.println();
		writeDb(new File("C:\\Users\\Lukas\\Desktop\\wordnet.sqlite"));
	}
	
	private static class WordNetDb extends DbConnection {

		public WordNetDb(File dbFile, boolean readOnly) {
			super(dbFile, readOnly);
		}
		
		@Override
		protected void createTables() {
			this.context.createTable("index_word")
				.column("lemma", SQLDataType.VARCHAR)
				.constraints(
					DSL.unique("lemma")
				)
				.execute();
			
			this.context.createTable("index_word_synset")
				.column("index_word", SQLDataType.VARCHAR)
				.column("synset", SQLDataType.INTEGER)
					.constraints(
						DSL.unique("index_word", "synset")
					)
				.execute();
			this.context.createIndex().on("index_word_synset", "index_word");
			this.context.createIndex().on("index_word_synset", "synset");
			
			this.context.createTable("synset")
				.column("offset", SQLDataType.VARCHAR)
				.constraints(
						DSL.unique("offset")
						)
				.execute();
			
			this.context.createTable("word")
				.column("key", SQLDataType.VARCHAR)
				.column("lemma", SQLDataType.VARCHAR)
				.column("synset", SQLDataType.INTEGER)
			.constraints(
				DSL.unique("key"),
				DSL.unique("lemma", "synset")
			)
			.execute();
			
			this.context.createTable("tuple")
				.column("instance_lemma", SQLDataType.VARCHAR)
				.column("instance_word", SQLDataType.VARCHAR)
				.column("instance_synset", SQLDataType.INTEGER)
				.column("class_lemma", SQLDataType.VARCHAR)
				.column("class_word", SQLDataType.VARCHAR)
				.column("class_synset", SQLDataType.INTEGER)
				.column("is_instance", SQLDataType.BOOLEAN)
				.column("is_transitive", SQLDataType.BOOLEAN.defaultValue(false))
				.constraints(
					DSL.unique("instance_word", "class_word")
				)
				.execute();
			this.context.createIndex().on("tuple", "instance_lemma");
			this.context.createIndex().on("tuple", "instance_word");
			this.context.createIndex().on("tuple", "class_lemma");
			this.context.createIndex().on("tuple", "class_word");
		}
		
		public void createIndexWord(IndexWord indexWord) throws SQLException {
			PreparedStatement stmt = this.connection.prepareStatement("INSERT INTO index_word (lemma) VALUES (?)");
			stmt.setString(1, indexWord.getLemma());
			stmt.execute();
		}
		
		public void createIndexWordSynsetConnection(IndexWord indexWord, Synset synset) throws SQLException {
			PreparedStatement stmt = this.connection.prepareStatement("INSERT INTO index_word_synset (index_word, synset) VALUES (?, ?)");
			stmt.setString(1, indexWord.getLemma());
			stmt.setLong(2, synset.getOffset());
			stmt.execute();
		}
		
		public void createSynset(Synset synset) throws SQLException {
			PreparedStatement stmt = this.connection.prepareStatement("INSERT INTO synset (offset) VALUES (?)");
			stmt.setLong(1, synset.getOffset());
			stmt.execute();
		}
		
		public void createWord(Word word) throws SQLException, JWNLException {
			PreparedStatement stmt = this.connection.prepareStatement("INSERT OR IGNORE INTO word (key, lemma, synset) VALUES (?, ?, ?)");
			stmt.setString(1, word.getSenseKey());
			stmt.setString(2, word.getLemma());
			stmt.setLong(3, word.getSynset().getOffset());
			stmt.execute();
		}
		
		public void createTuple(Word instanceWord, Word classWord, boolean isInstance) throws SQLException, JWNLException {
			PreparedStatement stmt = this.connection.prepareStatement("INSERT OR IGNORE INTO tuple (instance_lemma, instance_word, instance_synset, class_lemma, class_word, class_synset, is_instance) VALUES (?, ?, ?, ?, ?, ?, ?)");
			stmt.setString(1, instanceWord.getLemma());
			stmt.setString(2, instanceWord.getSenseKey());
			stmt.setLong(3, instanceWord.getSynset().getOffset());
			stmt.setString(4, classWord.getLemma());
			stmt.setString(5, classWord.getSenseKey());
			stmt.setLong(6, classWord.getSynset().getOffset());
			stmt.setBoolean(7, isInstance);
			stmt.execute();
		}
		
		public void calcTransitiveClosure() throws SQLException {
			this.connection.setAutoCommit(false);
			PreparedStatement connectionQuery = this.connection.prepareStatement("SELECT instance.instance_lemma, instance.instance_word, instance.instance_synset, instance.is_instance, class.class_lemma, class.class_word, class.class_synset FROM tuple AS instance, tuple AS class WHERE instance.class_synset = ? AND class.instance_synset = ?");
			PreparedStatement tupleInsertQuery = this.connection.prepareStatement("INSERT OR IGNORE INTO tuple (instance_lemma, instance_word, instance_synset, class_lemma, class_word, class_synset, is_instance, is_transitive) VALUES (?, ?, ?, ?, ?, ?, ?, 1)");
			int wordCount = DbUtils.getFirstRow(this.connection.createStatement().executeQuery("SELECT count(*) FROM synset")).getInt(1);
			ResultSet words = this.connection.createStatement().executeQuery("SELECT offset FROM synset");
			int i = 0;
			while (words.next()) {
				if (i % 100 == 0) {
					System.out.println(String.format("nodes %.2f %%", 100.0D * i / wordCount));
				}
				i++;
				String synset = words.getString(1);
				connectionQuery.setString(1, synset);
				connectionQuery.setString(2, synset);
				ResultSet connections = connectionQuery.executeQuery();
				while (connections.next()) {
					tupleInsertQuery.setString(1, connections.getString(1));
					tupleInsertQuery.setString(2, connections.getString(2));
					tupleInsertQuery.setLong(3, connections.getLong(3));
					tupleInsertQuery.setString(4, connections.getString(5));
					tupleInsertQuery.setString(5, connections.getString(6));
					tupleInsertQuery.setLong(6, connections.getLong(7));
					tupleInsertQuery.setBoolean(7, connections.getBoolean(4));
					tupleInsertQuery.execute();
				}
				this.connection.commit();
			}
			this.connection.setAutoCommit(true);
		}
		
	}

}
