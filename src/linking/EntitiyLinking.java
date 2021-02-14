package linking;

import java.io.File;
import java.sql.ResultSet;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

import db.DbConnection;
import utils.DbUtils;

public class EntitiyLinking {

	public static void countContainedRatio(int n) {
		Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "caligraph"));
		DbConnection crawlDb = new DbConnection(new File("D:\\db0.sqlite"), true) {};
		
		try (Session session = driver.session()) {
			Transaction tx = session.beginTransaction();
		
			int maxId = DbUtils.getFirstRow(crawlDb.connection.createStatement().executeQuery("SELECT seq FROM sqlite_sequence where name='lemmagroup'")).getInt(1);
			
			int found = 0;
			
			for (int i = 0; i < n; i++) {
				int lemmaId = (int) (Math.random() * maxId) + 1;
				String lemma = DbUtils.getFirstRow(crawlDb.connection.createStatement().executeQuery("SELECT words FROM lemmagroup WHERE id = " + lemmaId)).getString(1);
				System.out.println(lemma);
				Result result = tx.run("MATCH (n:Resource {rdfs__label:\"" +  lemma + "\"}) RETURN n LIMIT 1");
				if (result.hasNext()) {
					found++;
				}
				result.consume();
				System.out.println(found + " of " + i);
			}
			tx.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		driver.close();
	}
	
	public static void countContainedRatioSupport(int n, int s) {
		Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "caligraph"));
		DbConnection crawlDb = new DbConnection(new File("D:\\db\\db0.sqlite"), true) {};
		DbConnection tupleDb = new DbConnection(new File("D:\\db\\medium\\relations.sqlite"), true) {};
		
		try (Session session = driver.session()) {
			Transaction tx = session.beginTransaction();
		
			ResultSet matches = tupleDb.connection.createStatement().executeQuery("SELECT instance, class FROM lemma_pair WHERE pld_count >= " + s);
			
			int foundClass = 0;
			int foundInstance = 0;
			int foundBoth = 0;
			int foundTuple = 0;
			
			for (int i = 0; i < n; i++) {
				matches.next();
				int instanceId = matches.getInt(1);
				int classId = matches.getInt(2);
				String instance = DbUtils.getFirstRow(crawlDb.connection.createStatement().executeQuery("SELECT words FROM lemmagroup WHERE id = " + instanceId)).getString(1);
				String classStr = DbUtils.getFirstRow(crawlDb.connection.createStatement().executeQuery("SELECT words FROM lemmagroup WHERE id = " + classId)).getString(1);
				System.out.println(instance + "  " + classStr);
				Result result = tx.run("MATCH (n:Resource {rdfs__label:\"" +  instance + "\"}) RETURN n LIMIT 1");
				boolean hasClass = result.hasNext();
				result = tx.run("MATCH (n:Resource {rdfs__label:\"" +  classStr + "\"}) RETURN n LIMIT 1");
				boolean hasInstance = result.hasNext();
				if (hasClass) {
					foundClass++;
				}
				if (hasInstance) {
					foundInstance++;
				}
				if (hasClass && hasInstance) {
					foundBoth++;
				}
				result = tx.run("MATCH p = (n:Resource {rdfs__label:\"" +  instance + "\"})-->(m {label:\"" + classStr + "\"}) RETURN p LIMIT 1");
				if (result.hasNext()) {
					foundTuple++;
				}
				System.out.println(i + " " + foundClass + " " + foundInstance + " " + foundBoth + " " + foundTuple);
			}
			tx.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		driver.close();
	}
	
	public static void main(String[] args) {
		countContainedRatioSupport(100, 50);
	}

}
