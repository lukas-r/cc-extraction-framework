package test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import db.DbConnection;
import db.ExtractionInserter;
import db.Merger;
import db.PairType;
import db.RelationIterator;
import db.RelationIterator.Step;
import evaluation.CountAttribute;
import evaluation.PairTest;
import evaluation.PairTest.TestType;
import queue.TextQueue;
import taxonomy.TaxonomyCreator;
import taxonomy.TaxonomyUtils;

public class TestDb {
	
	public static void testExtractionInsert() {
		ExtractionInserter inserter = new ExtractionInserter(new File("D:\\db0.sqlite"), new TextQueue(new File("C:\\Users\\Lukas\\Desktop\\db\\queue")), true);
		inserter.insert("CC-MAIN-2020-34");
		inserter.close();
	}
	
	public static void testMerger() {
		Merger merger = new Merger(
				new File("D:\\db\\db0.sqlite"),
				new File("D:\\db\\db1.sqlite"),
				new File("D:\\merged_0_1.sqlite"),
				new File("D:\\tmp\\")
		);
		merger.merge();
	}
	
	public static void runIterator() {
		RelationIterator iterator = new RelationIterator(
			new File("D:\\db\\db0.sqlite"),
			new File("D:\\db\\medium\\relations.sqlite"),
			new File("D:\\db\\medium\\helper.sqlite")
		);
		try {
			//iterator.iterate(Step.WRITE_CANDIDATES, false);
			//iterator.iterate(Step.ONE_TO_ONE, false);
			//iterator.iterate(Step.ONE_TO_MANY, false);
			iterator.iterate(Step.MANY_TO_ANY, false);
			iterator.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void changeColumnName() {

	}
	
	public static void addColumn() {
		DbConnection db = new DbConnection(new File("D:\\db0.sqlite")) {};
		try {
			db.connection.createStatement().execute("ALTER TABLE instance ADD lemmagroup INTEGER;");
			db.connection.createStatement().execute("ALTER TABLE class ADD lemmagroup INTEGER;");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		db.close();
	}
	
	public static void addExtractionIndexes() {
		DbConnection db = new DbConnection(new File("db0.sqlite")) {};
		int i = 0;
		System.out.println(i++);
		db.context.createIndex().on("page", "pld").execute();
		System.out.println(i++);
		db.context.createIndex().on("page", "url").execute();
		System.out.println(i++);
		db.context.createIndex().on("page", "crawl").execute();
		System.out.println(i++);
		db.context.createIndex().on("page", "file").execute();
		System.out.println(i++);
		db.context.createIndex().on("page", "crawl", "file").execute();
		System.out.println(i++);
		
		db.context.createIndex().on("page_sentence", "page").execute();
		System.out.println(i++);
		db.context.createIndex().on("page_sentence", "pld").execute();
		System.out.println(i++);
		db.context.createIndex().on("page_sentence", "sentence").execute();
		System.out.println(i++);
		db.context.createIndex().on("page_sentence", "page", "sentence").execute();
		System.out.println(i++);
		db.context.createIndex().on("page_sentence", "page", "pld", "sentence").execute();
		System.out.println(i++);
		
		db.context.createIndex().on("matching", "sentence").execute();
		System.out.println(i++);
		db.context.createIndex().on("matching", "pattern").execute();
		System.out.println(i++);
		
		db.context.createIndex().on("premod", "words").execute();
		System.out.println(i++);
		
		db.context.createIndex().on("postmod", "words").execute();
		System.out.println(i++);
		
		db.context.createIndex().on("noungroup", "words").execute();
		System.out.println(i++);
		db.context.createIndex().on("noungroup", "lemmas").execute();
		System.out.println(i++);
		
		db.context.createIndex().on("term", "premod").execute();
		System.out.println(i++);
		db.context.createIndex().on("term", "postmod").execute();
		System.out.println(i++);
		db.context.createIndex().on("term", "noungroup").execute();
		System.out.println(i++);
		
		db.context.createIndex().on("matching_info", "instance_count").execute();
		System.out.println(i++);
		db.context.createIndex().on("matching_info", "class_count").execute();
		System.out.println(i++);
		
		db.context.createIndex().on("instance", "matching").execute();
		System.out.println(i++);
		db.context.createIndex().on("instance", "term").execute();
		System.out.println(i++);
		db.context.createIndex().on("instance", "lemmagroup").execute();
		System.out.println(i++);
		
		db.context.createIndex().on("class", "matching").execute();
		System.out.println(i++);
		db.context.createIndex().on("class", "term").execute();
		System.out.println(i++);
		db.context.createIndex().on("class", "lemmagroup").execute();
		System.out.println(i++);
		
		db.close();
	}
	
	public static void addIndexes() {
		DbConnection db = new DbConnection(new File("D:\\db\\medium\\relations.sqlite")) {};

		db.context.createIndex().on("term_pair", "count").execute();
		db.context.createIndex().on("term_pair", "pld_count").execute();
		db.context.createIndex().on("lemma_pair", "count").execute();
		db.context.createIndex().on("lemma_pair", "pld_count").execute();
		
		db.close();
	}
	
	public static void testTest() {
		long time = System.nanoTime();
		try {
			PairTest.evaluate(
//				new File("D:\\db\\medium\\relations.sqlite"),
//				new File("D:\\db\\db0.sqlite"),
//				new File("D:\\results.sqlite"),
					new File("C:\\Users\\Lukas\\Desktop\\db\\small\\relations.sqlite"),
				new File("C:\\Users\\Lukas\\Desktop\\db\\small\\db.sqlite"),
				new File("C:\\Users\\Lukas\\Desktop\\db\\small\\results.sqlite"),
				TestType.PER_PATTERN, PairType.LEMMA, CountAttribute.FREQUENCY, 10, 23, true
			);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(System.nanoTime() - time);
	}
	
	public static void testTaxonmyCreator() {
		TaxonomyCreator creator;
		try {
			creator = new TaxonomyCreator(
				new File("C:\\Users\\Lukas\\Desktop\\db\\small\\taxonomy.sqlite"),
				new File("C:\\Users\\Lukas\\Desktop\\db\\small\\helper.sqlite"),
				new File("C:\\Users\\Lukas\\Desktop\\db\\small\\")
			);
			creator.create();
			creator.close();
		} catch (IOException | SQLException | InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	public static void testExportNeo4jCsv() {
		TaxonomyUtils utils = new TaxonomyUtils(new File("D:\\db\\taxonomy\\taxonomy.sqlite"));
		try {
			utils.exportNeo4JCsv(
				new File("D:\\db\\db0.sqlite"),
				new File("D:\\db\\taxonomy\\"),
				PairType.TERM);
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void testExportEdgeList() {
		TaxonomyUtils utils = new TaxonomyUtils(new File("C:\\Users\\Lukas\\Desktop\\db\\small\\taxonomy - Kopie.sqlite"));
		try {
			utils.exportEdgeList(new File("C:\\Users\\Lukas\\Desktop\\db\\small\\term_edgelist"), PairType.TERM);
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		//testTest();
		//runIterator();
		//addIndexes();
		addExtractionIndexes();
		//testTaxonmyCreator();
		//testExportNeo4jCsv();
		//testExportEdgeList();
//		try {
//			new DbConnection(new File("D:\\test")) {
//			}.connection.setAutoCommit(false);
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
