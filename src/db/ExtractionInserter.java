package db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import org.jooq.InsertQuery;
import org.jooq.InsertValuesStep6;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import extraction.output.DuplicateOutputObject;
import extraction.output.MatchOutputObject;
import extraction.output.SentenceOutputObject;
import extraction.output.TermOutputObject;
import extraction.pattern.Pattern;
import extraction.pattern.StandardPatterns;
import queue.Queue;

public class ExtractionInserter extends DbConnection {
	
	private final Queue<String> queue;
	private final boolean zipped;
	private final Map<String, Integer> patternMap;
	
	private File stopFile;

	@SuppressWarnings("rawtypes")
	private InsertQuery fileInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery pldInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery pageInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery sentenceInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery pageSentenceInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery matchingInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery premodInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery postmodInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery noungroupInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery lemmagroupInsertQuery;
	/*@SuppressWarnings("rawtypes")
	private InsertQuery lemmaPairInsertQuery;*/
	@SuppressWarnings("rawtypes")
	private InsertQuery termInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery matchingInfoInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery instanceInsertQuery;
	@SuppressWarnings("rawtypes")
	private InsertQuery classInsertQuery;
	/*@SuppressWarnings("rawtypes")
	private InsertQuery matchInsertQuery;*/
	
	@SuppressWarnings("rawtypes")
	private ResultQuery patternSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery fileSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery pldSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery pageSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery sentenceSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery pageSentenceSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery matchingSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery premodSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery postmodSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery noungroupSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery lemmagroupSelectQuery;
	/*@SuppressWarnings("rawtypes")
	private ResultQuery lemmaPairSelectQuery;*/
	@SuppressWarnings("rawtypes")
	private ResultQuery termSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery matchingInfoSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery instanceSelectQuery;
	@SuppressWarnings("rawtypes")
	private ResultQuery classSelectQuery;
	/*@SuppressWarnings("rawtypes")
	private ResultQuery matchSelectQuery;*/
	
	public ExtractionInserter(File dbFile, Queue<String> queue, boolean zipped) {
		super(dbFile);
		System.out.println(dbFile.getName());
		this.queue = queue;
		this.zipped = zipped;
		this.patternMap = new HashMap<String, Integer>();
	}

	protected void createTables() {
		this.context.createTable("crawl")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("crawl", SQLDataType.VARCHAR)
			.constraints(
				DSL.unique("crawl")
			)
			.execute();
		
		this.context.createTable("file")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("crawl", SQLDataType.INTEGER)
			.column("file", SQLDataType.VARCHAR)
			.constraints(
				DSL.unique("crawl", "file"),
				DSL.foreignKey("crawl").references("crawl")
			)
			.execute();
//		this.context.createIndex().on("file", "file").execute();
		
		this.context.createTable("pld")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("pld", SQLDataType.VARCHAR)
			.constraints(
				DSL.unique("pld")
			)
			.execute();
		
		this.context.createTable("page")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("url", SQLDataType.VARCHAR)
			.column("pld", SQLDataType.INTEGER)
			.column("crawl", SQLDataType.INTEGER)
			.column("file", SQLDataType.INTEGER)
			.column("title", SQLDataType.VARCHAR)
			.column("date", SQLDataType.VARCHAR)
			.constraints(
				DSL.unique("url", "file"),
				DSL.foreignKey("pld").references("pld"),
				DSL.foreignKey("crawl").references("crawl"),
				DSL.foreignKey("file").references("file")
			)
			.execute();
//		this.context.createIndex().on("page", "pld").execute();
//		this.context.createIndex().on("page", "url").execute();
//		this.context.createIndex().on("page", "crawl").execute();
//		this.context.createIndex().on("page", "file").execute();
//		this.context.createIndex().on("page", "crawl", "file").execute();
		
		this.context.createTable("sentence")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("sentence", SQLDataType.CLOB)
			.constraints(
				DSL.unique("sentence")
			)
			.execute();
		
		this.context.createTable("page_sentence")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("page", SQLDataType.INTEGER)
			.column("pld", SQLDataType.INTEGER)
			.column("sentence", SQLDataType.INTEGER)
			.constraints(
				DSL.unique("pld", "sentence"),
				DSL.foreignKey("page").references("page"),
				DSL.foreignKey("pld").references("pld"),
				DSL.foreignKey("sentence").references("sentence")
			)
			.execute();
//		this.context.createIndex().on("page_sentence", "page").execute();
//		this.context.createIndex().on("page_sentence", "pld").execute();
//		this.context.createIndex().on("page_sentence", "sentence").execute();
//		this.context.createIndex().on("page_sentence", "page", "sentence").execute();
//		this.context.createIndex().on("page_sentence", "page", "pld", "sentence").execute();
		
		this.context.createTable("pattern")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("name", SQLDataType.VARCHAR)
			.column("type", SQLDataType.VARCHAR)
			.column("order", SQLDataType.VARCHAR)
			.column("instance_numerus", SQLDataType.VARCHAR)
			.column("class_numerus", SQLDataType.VARCHAR)
			.column("instance_count", SQLDataType.VARCHAR)
			.constraints(
				DSL.unique("name")
			)
			.execute();
		
		this.context.createTable("matching")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("sentence", SQLDataType.INTEGER)
			.column("pattern", SQLDataType.INTEGER)
			.column("pos", SQLDataType.INTEGER)
			.constraints(
				DSL.unique("sentence", "pattern", "pos"),
				DSL.foreignKey("sentence").references("sentence"),
				DSL.foreignKey("pattern").references("pattern")
			)
			.execute();
//		this.context.createIndex().on("matching", "sentence").execute();
//		this.context.createIndex().on("matching", "pattern").execute();
		
		this.context.createTable("premod")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("words", SQLDataType.VARCHAR)
			.column("tags", SQLDataType.VARCHAR)
			.constraints(
				DSL.unique("words", "tags")
			)
			.execute();
		this.context.createIndex().on("premod", "words");
//		this.context.createIndex().on("premod", "words").execute();
		
		this.context.createTable("postmod")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("words", SQLDataType.VARCHAR)
			.column("tags", SQLDataType.VARCHAR)
			.constraints(
				DSL.unique("words", "tags")
			)
			.execute();
//		this.context.createIndex().on("postmod", "words").execute();
		
		this.context.createTable("noungroup")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("words", SQLDataType.VARCHAR)
			.column("tags", SQLDataType.VARCHAR)
			.column("lemmas", SQLDataType.INTEGER)
			.constraints(
				DSL.unique("words", "tags", "lemmas"),
				DSL.foreignKey("lemmas").references("lemmagroup")
			)
			.execute();
//		this.context.createIndex().on("noungroup", "words").execute();
//		this.context.createIndex().on("noungroup", "lemmas").execute();
		
		this.context.createTable("lemmagroup")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("words", SQLDataType.VARCHAR)
			.constraints(
				DSL.unique("words")
			)
			.execute();
		
		this.context.createTable("term")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("premod", SQLDataType.INTEGER)
			.column("postmod", SQLDataType.INTEGER)
			.column("noungroup", SQLDataType.INTEGER)
			.constraints(
				DSL.unique("premod", "postmod", "noungroup"),
				DSL.foreignKey("premod").references("premod"),
				DSL.foreignKey("postmod").references("postmod"),
				DSL.foreignKey("noungroup").references("noungroup")
			)
			.execute();
//		this.context.createIndex().on("term", "premod").execute();
//		this.context.createIndex().on("term", "postmod").execute();
//		this.context.createIndex().on("term", "noungroup").execute();
		
		this.context.createTable("matching_info")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("matching", SQLDataType.INTEGER)
			.column("instance_count", SQLDataType.INTEGER)
			.column("class_count", SQLDataType.INTEGER)
			.constraints(
					DSL.unique("matching"),
					DSL.foreignKey("matching").references("matching")
				)
				.execute();
//		this.context.createIndex().on("matching_info", "instance_count").execute();
//		this.context.createIndex().on("matching_info", "class_count").execute();
		
		this.context.createTable("instance")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("matching", SQLDataType.INTEGER)
			.column("term", SQLDataType.INTEGER)
			.column("lemmagroup", SQLDataType.INTEGER)
			.column("depth", SQLDataType.INTEGER)
			.column("no", SQLDataType.INTEGER)
			.column("pos", SQLDataType.INTEGER)
			.column("combined", SQLDataType.BOOLEAN)
			.constraints(
				DSL.unique("matching", "term", "pos", "depth", "no", "combined"),
				DSL.foreignKey("matching").references("matching"),
				DSL.foreignKey("term").references("term"),
				DSL.foreignKey("lemmagroup").references("lemmagroup")
			)
			.execute();
//		this.context.createIndex().on("instance", "matching").execute();
//		this.context.createIndex().on("instance", "term").execute();
//		this.context.createIndex().on("instance", "lemmagroup").execute();
		
		this.context.createTable("class")
			.column("id", SQLDataType.INTEGER.identity(true))
			.column("matching", SQLDataType.INTEGER)
			.column("term", SQLDataType.INTEGER)
			.column("lemmagroup", SQLDataType.INTEGER)
			.column("depth", SQLDataType.INTEGER)
			.column("no", SQLDataType.INTEGER)
			.column("pos", SQLDataType.INTEGER)
			.column("combined", SQLDataType.BOOLEAN)
			.constraints(
				DSL.unique("matching", "term", "pos", "depth", "no", "combined"),
				DSL.foreignKey("matching").references("matching"),
				DSL.foreignKey("lemmagroup").references("lemmagroup")
			)
			.execute();
//		this.context.createIndex().on("class", "matching").execute();
//		this.context.createIndex().on("class", "term").execute();
//		this.context.createIndex().on("class", "lemmagroup").execute();
	}
	
	protected void initialPrepare() {
		this.insertPatterns();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void insertPatterns() {
		this.context.transaction(configuration -> {		
			InsertValuesStep6 patternQuery = this.context.insertInto(DSL.table("pattern"), DSL.field(DSL.name("name")), DSL.field(DSL.name("type")), DSL.field(DSL.name("order")), DSL.field(DSL.name("class_numerus")), DSL.field(DSL.name("instance_numerus")), DSL.field(DSL.name("instance_count")));
			for (Pattern pattern: StandardPatterns.LIST) {
				patternQuery = patternQuery.values(pattern.name, pattern.type.name(), pattern.order.name(), pattern.classNumerus.name(), pattern.instanceNumerus.name(), pattern.instanceCount.name());
			}
			patternQuery.execute();
		});
	}
	
	private int getCrawlId(String crawl) {
		AtomicReference<Integer> crawlId = new AtomicReference<Integer>();
		this.context.transaction(configuration -> {
			crawlId.set((Integer) this.context.select(DSL.field(DSL.name("id"))).from("crawl").fetchOne("id"));
			if (crawlId.get() == null) {
				crawlId.set((Integer) this.context.insertInto(DSL.table("crawl"), DSL.field(DSL.name("crawl"))).values(crawl).returningResult(DSL.field(DSL.name("id"))).fetchOne().value1());
			}
		});
		return crawlId.get();
	}
	
	public static File getStopFile(File dbPath) {
		return new File(dbPath.getAbsolutePath() + File.separator + "stop");
	}
	
	private File getStopFile() {
		if (this.stopFile == null) {
			this.stopFile = getStopFile(this.dbFile.getParentFile());
		}
		return this.stopFile;
	}
	
	public boolean stopFileExists() {
		return this.getStopFile().exists();
	}
	
	public void insert(String crawl) {
		int i = 0;
		String name = "inserter";
		try {
			this.connection.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}
		
		int crawlId = this.getCrawlId(crawl);
		
		String fileString;
		while ((fileString = this.queue.pop(name)) != null) {
			File file = new File(fileString);
			System.out.println(this.dbFile.getName() + "\t" + i++ + "\t\t" + file.getName());
			InputStream input;
			try {
				this.openQueries();
				ReturnObject fileReturn = this.insertFile(crawlId, file.getName());
				if (!fileReturn.created) {
					this.queue.done(fileString, name);
					this.closeQueries();
					continue;
				}
				input = zipped ? new GZIPInputStream(new FileInputStream(file)) : new FileInputStream(file);
				BufferedReader reader = new BufferedReader(new InputStreamReader(input));
				
				String line;
				ReturnObject sentenceReturnObject = null;
				int patternId = -1;
				ReturnObject matchingReturnObject = null;
				int classCount = 0;
				int instanceCount = 0;
				while ((line = reader.readLine()) != null) {
					String[] lineParts = line.split("\t", -1);
					switch (lineParts[0].toUpperCase()) {
						case "S":
							SentenceOutputObject sentenceObject = SentenceOutputObject.fromLineParts(lineParts);
							sentenceReturnObject = insertSentenceObject(crawlId, fileReturn.id, sentenceObject);
							patternId = getPatternId(sentenceObject.pattern);
							break;
						case "M":
							if (!sentenceReturnObject.created) {
								break;
							}
							if (matchingReturnObject != null) {
								insertMatchingInfo(matchingReturnObject.id, classCount, instanceCount);
							}
							classCount = 0;
							instanceCount = 0;
							MatchOutputObject matchingObject = MatchOutputObject.fromLineParts(lineParts);
							matchingReturnObject = insertMatchingObject(sentenceReturnObject.id, patternId, matchingObject);
							break;
						case "C":
							if (!sentenceReturnObject.created || !matchingReturnObject.created) {
								break;
							}
							TermOutputObject classObject = TermOutputObject.fromLineParts(lineParts);
							insertTermObject(matchingReturnObject.id, classObject);
							classCount++;
							break;
						case "I":
							if (!sentenceReturnObject.created || !matchingReturnObject.created) {
								break;
							}
							TermOutputObject instanceObject = TermOutputObject.fromLineParts(lineParts);
							insertTermObject(matchingReturnObject.id, instanceObject);
							instanceCount++;
							break;
						case "D":
							DuplicateOutputObject duplicateObject = DuplicateOutputObject.fromLineParts(lineParts);
							insertDuplicate(crawlId, fileReturn.id, duplicateObject);
							break;
						default:
							throw new Exception("Wrong line type " + lineParts[0]);
					}
				}
				insertMatchingInfo(matchingReturnObject.id, classCount, instanceCount);
				
				this.connection.commit();
				this.queue.done(fileString, name);
				this.closeQueries();
				
				if (this.stopFileExists()) {
					System.out.println("STOP");
					this.close();
					break;
				}
			} catch (Exception e) {
				System.out.println("error in file " + file.getName());
				e.printStackTrace();
				this.queue.back(fileString, false);
				
				try {
					this.connection.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void openQueries() {
		this.patternSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("pattern")
				.where(DSL.field(DSL.name("name")).eq(DSL.param("name")))
				.keepStatement(true);
		
		this.fileInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("file"), DSL.field(DSL.name("crawl")), DSL.field(DSL.name("file")))
				.values(DSL.param("crawl"), DSL.param("file"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.fileSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("file")
				.where(DSL.field(DSL.name("crawl")).eq(DSL.param("crawl")), DSL.field(DSL.name("file")).eq(DSL.param("file")))
				.keepStatement(true);
		
		this.pldInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("pld"), DSL.field(DSL.name("pld")))
				.values(DSL.param("pld"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.pldSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("pld")
				.where(DSL.field(DSL.name("pld")).eq(DSL.param("pld")))
				.keepStatement(true);
		
		this.pageInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("page"), DSL.field(DSL.name("url")), DSL.field(DSL.name("pld")), DSL.field(DSL.name("crawl")), DSL.field(DSL.name("file")), DSL.field(DSL.name("title")), DSL.field(DSL.name("date")))
				.values(DSL.param("url"), DSL.param("pld"), DSL.param("crawl"), DSL.param("file"), DSL.param("title"), DSL.param("date"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.pageSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("page")
				.where(DSL.field(DSL.name("file")).eq(DSL.param("file")), DSL.field(DSL.name("url")).eq(DSL.param("url")))
				.keepStatement(true);
		
		this.sentenceInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("sentence"), DSL.field(DSL.name("sentence")))
				.values(DSL.param("sentence"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.sentenceSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("sentence")
				.where(DSL.field(DSL.name("sentence")).eq(DSL.param("sentence")))
				.keepStatement(true);
		
		this.pageSentenceInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("page_sentence"), DSL.field(DSL.name("page")), DSL.field(DSL.name("pld")), DSL.field(DSL.name("sentence")))
				.values(DSL.param("page"), DSL.param("pld"), DSL.param("sentence"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.pageSentenceSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("page_sentence")
				.where(DSL.field(DSL.name("pld")).eq(DSL.param("pld")), DSL.field(DSL.name("sentence")).eq(DSL.param("sentence")))
				.keepStatement(true);

		this.matchingInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("matching"), DSL.field(DSL.name("sentence")), DSL.field(DSL.name("pattern")), DSL.field(DSL.name("pos")))
				.values(DSL.param("sentence"), DSL.param("pattern"), DSL.param("pos"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.matchingSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("matching")
				.where(DSL.field(DSL.name("sentence")).eq(DSL.param("sentence")), DSL.field(DSL.name("pattern")).eq(DSL.param("pattern")), DSL.field(DSL.name("pos")).eq(DSL.param("pos")))
				.keepStatement(true);
		
		this.premodInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("premod"), DSL.field(DSL.name("words")), DSL.field(DSL.name("tags")))
				.values(DSL.param("words"), DSL.param("tags"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.premodSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("premod")
				.where(DSL.field(DSL.name("words")).eq(DSL.param("words")), DSL.field(DSL.name("tags")).eq(DSL.param("tags")))
				.keepStatement(true);
		
		this.postmodInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("postmod"), DSL.field(DSL.name("words")), DSL.field(DSL.name("tags")))
				.values(DSL.param("words"), DSL.param("tags"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.postmodSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("postmod")
				.where(DSL.field(DSL.name("words")).eq(DSL.param("words")), DSL.field(DSL.name("tags")).eq(DSL.param("tags")))
				.keepStatement(true);
		
		this.noungroupInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("noungroup"), DSL.field(DSL.name("words")), DSL.field(DSL.name("tags")), DSL.field(DSL.name("lemmas")))
				.values(DSL.param("words"), DSL.param("tags"), DSL.param("lemmas"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.noungroupSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("noungroup")
				.where(DSL.field(DSL.name("words")).eq(DSL.param("words")), DSL.field(DSL.name("tags")).eq(DSL.param("tags")), DSL.field(DSL.name("lemmas")).eq(DSL.param("lemmas")))
				.keepStatement(true);

		
		this.lemmagroupInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("lemmagroup"), DSL.field(DSL.name("words")))
				.values(DSL.param("words"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.lemmagroupSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("lemmagroup")
				.where(DSL.field(DSL.name("words")).eq(DSL.param("words")))
				.keepStatement(true);
		/*this.lemmaPairInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("lemma_pair"), DSL.field(DSL.name("instance")), DSL.field(DSL.name("class")))
				.values(DSL.param("instance"), DSL.param("class"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.lemmaPairSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("lemma_pair")
				.where(DSL.field(DSL.name("instance")).eq(DSL.param("instance")), DSL.field(DSL.name("class")).eq(DSL.param("class")))
				.keepStatement(true);*/
		
		
		this.termInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("term"), DSL.field(DSL.name("premod")), DSL.field(DSL.name("postmod")), DSL.field(DSL.name("noungroup")))
				.values(DSL.param("premod"), DSL.param("postmod"), DSL.param("noungroup"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.termSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("term")
				.where(DSL.field(DSL.name("premod")).eq(DSL.param("premod")), DSL.field(DSL.name("postmod")).eq(DSL.param("postmod")), DSL.field(DSL.name("noungroup")).eq(DSL.param("noungroup")))
				.keepStatement(true);
		this.matchingInfoInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("matching_info"), DSL.field(DSL.name("matching")), DSL.field(DSL.name("instance_count")), DSL.field(DSL.name("class_count")))
				.values(DSL.param("matching"), DSL.param("instance_count"), DSL.param("class_count"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.matchingInfoSelectQuery = this.context.select(DSL.field(DSL.name("id")), DSL.field(DSL.name("instance_count")), DSL.field(DSL.name("class_count")))
				.from("matching_info")
				.where(DSL.field(DSL.name("matching")).eq(DSL.param("matching")))
				.keepStatement(true);
		this.instanceInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("instance"), DSL.field(DSL.name("matching")), DSL.field(DSL.name("term")), DSL.field(DSL.name("lemmagroup")), DSL.field(DSL.name("no")), DSL.field(DSL.name("pos")), DSL.field(DSL.name("depth")), DSL.field(DSL.name("combined")))
				.values(DSL.param("matching"), DSL.param("term"), DSL.param("lemmagroup"), DSL.param("no"), DSL.param("pos"), DSL.param("depth"), DSL.param("combined"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.instanceSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("instance")
				.where(DSL.field(DSL.name("matching")).eq(DSL.param("matching")), DSL.field(DSL.name("term")).eq(DSL.param("term")), DSL.field(DSL.name("no")).eq(DSL.param("no")), DSL.field(DSL.name("pos")).eq(DSL.param("pos")), DSL.field(DSL.name("depth")).eq(DSL.param("depth")), DSL.field(DSL.name("combined")).eq(DSL.param("combined")))
				.keepStatement(true);
		this.classInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("class"), DSL.field(DSL.name("matching")), DSL.field(DSL.name("term")), DSL.field(DSL.name("lemmagroup")), DSL.field(DSL.name("no")), DSL.field(DSL.name("pos")), DSL.field(DSL.name("depth")), DSL.field(DSL.name("combined")))
				.values(DSL.param("matching"), DSL.param("term"), DSL.param("lemmagroup"), DSL.param("no"), DSL.param("pos"), DSL.param("depth"), DSL.param("combined"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);
		this.classSelectQuery = this.context.select(DSL.field(DSL.name("id")))
				.from("class")
				.where(DSL.field(DSL.name("matching")).eq(DSL.param("matching")), DSL.field(DSL.name("term")).eq(DSL.param("term")), DSL.field(DSL.name("no")).eq(DSL.param("no")), DSL.field(DSL.name("pos")).eq(DSL.param("pos")), DSL.field(DSL.name("depth")).eq(DSL.param("depth")), DSL.field(DSL.name("combined")).eq(DSL.param("combined")))
				.keepStatement(true);
		/*this.matchInsertQuery = (InsertQuery) this.context.insertInto(DSL.table("match"), DSL.field(DSL.name("matching")), DSL.field(DSL.name("instance")), DSL.field(DSL.name("class")), DSL.field(DSL.name("lemma_pair")), DSL.field(DSL.name("pos")), DSL.field(DSL.name("depth")), DSL.field(DSL.name("combined")))
				.values(DSL.param("page"), DSL.param("pld"), DSL.param("sentence"), DSL.param("sentence"), DSL.param("sentence"), DSL.param("sentence"))
				.returning(DSL.field(DSL.name("id")))
				.keepStatement(true);*/
	}
	
	private void closeQueries() {
		this.pldInsertQuery.close();
		this.pldSelectQuery.close();
		this.pageInsertQuery.close();
		this.pageSelectQuery.close();
		this.sentenceInsertQuery.close();
		this.sentenceSelectQuery.close();
		this.pageSentenceInsertQuery.close();
		this.pageSentenceSelectQuery.close();
		this.matchingInsertQuery.close();
		this.matchingSelectQuery.close();
		this.premodInsertQuery.close();
		this.premodSelectQuery.close();
		this.postmodInsertQuery.close();
		this.postmodSelectQuery.close();
		this.noungroupInsertQuery.close();
		this.noungroupSelectQuery.close();
		//this.lemmagroupInsertQuery.close();
		//this.lemmagroupSelectQuery.close();
		//this.lemmaPairInsertQuery.close();
		//this.lemmaPairSelectQuery.close();
		this.termInsertQuery.close();
		this.termSelectQuery.close();
		this.matchingInfoInsertQuery.close();
		this.matchingInfoSelectQuery.close();
		this.instanceInsertQuery.close();
		this.instanceSelectQuery.close();
		this.classInsertQuery.close();
		this.classSelectQuery.close();
		//this.matchInsertQuery.close();
		//this.matchSelectQuery.close();
	}
	
	private int getPatternId(String name) {
		Integer patternId = this.patternMap.get(name);
		if (patternId == null) {
			this.patternSelectQuery.bind("name", name);
			patternId = (Integer) this.patternSelectQuery.fetchOne("id");
			this.patternMap.put(name, patternId);
		}
		return patternId;
	}
	
	private ReturnObject insertFile(int crawlId, String file) {
		this.fileSelectQuery.bind("crawl", crawlId);
		this.fileSelectQuery.bind("file", file);
		Integer fileId = (Integer) this.fileSelectQuery.fetchOne("id");
		boolean created = false;
		if (fileId == null) {
			created = true;
			this.fileInsertQuery.bind("crawl", crawlId);
			this.fileInsertQuery.bind("file", file);
			this.fileInsertQuery.execute();
			fileId = (Integer) this.fileInsertQuery.getReturnedRecord().getValue("id");	
		}
		return new ReturnObject(crawlId, created);
	}
	
	private int insertPld(int fileId, String pld) {
		this.pldSelectQuery.bind("pld", pld);
		Integer pldId = (Integer) this.pldSelectQuery.fetchOne("id");
		if (pldId == null) {
			this.pldInsertQuery.bind("pld", pld);
			this.pldInsertQuery.execute();
			pldId = (Integer) this.pldInsertQuery.getReturnedRecord().getValue("id");
		}
		return pldId;
	}
	
	private int insertPage(int crawlId, int fileId, String url, int pldId) {
		this.pageSelectQuery.bind("file", fileId);
		this.pageSelectQuery.bind("url", url);
		Integer pageId = (Integer) this.pageSelectQuery.fetchOne("id");
		if (pageId == null) {
			this.pageInsertQuery.bind("crawl", crawlId);
			this.pageInsertQuery.bind("file", fileId);
			this.pageInsertQuery.bind("url", url);
			this.pageInsertQuery.bind("pld", pldId);
			this.pageInsertQuery.execute();
			pageId = (Integer) this.pageInsertQuery.getReturnedRecord().getValue("id");
		}
		return pageId;
	}
	
	private ReturnObject insertSentence(String sentence) {
		this.sentenceSelectQuery.bind("sentence", sentence);
		Integer sentenceId = (Integer) this.sentenceSelectQuery.fetchOne("id");
		boolean created = false;
		if (sentenceId == null) {
			created = true;
			this.sentenceInsertQuery.bind("sentence", sentence);
			this.sentenceInsertQuery.execute();
			sentenceId = (Integer) this.sentenceInsertQuery.getReturnedRecord().getValue("id");
		}
		return new ReturnObject(sentenceId, created);
	}
	
	private int insertPageSentence(int pldId, int pageId, int sentenceId) {
		this.pageSentenceSelectQuery.bind("pld", pldId);
		this.pageSentenceSelectQuery.bind("sentence", sentenceId);
		Integer pageSentenceId = (Integer) this.pageSentenceSelectQuery.fetchOne("id");
		if (pageSentenceId == null) {
			this.pageSentenceInsertQuery.bind("pld", pldId);
			this.pageSentenceInsertQuery.bind("page", pageId);
			this.pageSentenceInsertQuery.bind("sentence", sentenceId);
			this.pageSentenceInsertQuery.execute();
			pageSentenceId = (Integer) this.pageSentenceInsertQuery.getReturnedRecord().getValue("id");
		}
		return pageSentenceId;
	}
	
	private ReturnObject insertSentenceObject(int crawlId, int fileId, SentenceOutputObject object) {
		int pldId = insertPld(fileId, object.pld);
		int pageId = insertPage(crawlId, fileId, object.url, pldId);
		ReturnObject sentenceReturnObject = insertSentence(object.sentence);
		insertPageSentence(pldId, pageId, sentenceReturnObject.id);
		return sentenceReturnObject;
	}
	
	private void insertMatchingInfo(int matchingId, int classCount, int instanceCount) {
		this.matchingInfoSelectQuery.bind("matching", matchingId);
		Record result = this.matchingInfoSelectQuery.fetchOne();
		if (result == null) {			
			this.matchingInfoInsertQuery.bind("matching", matchingId);
			this.matchingInfoInsertQuery.bind("class_count", classCount);
			this.matchingInfoInsertQuery.bind("instance_count", instanceCount);
			this.matchingInfoInsertQuery.execute();
		}
	}
	
	private ReturnObject insertMatching(int sentenceId, int patternId, int position) {
		this.matchingSelectQuery.bind("sentence", sentenceId);
		this.matchingSelectQuery.bind("pattern", patternId);
		this.matchingSelectQuery.bind("pos", position);
		Integer matchingId = (Integer) this.matchingSelectQuery.fetchOne("id");
		boolean created = false;
		if (matchingId == null) {
			created = true;
			this.matchingInsertQuery.bind("sentence", sentenceId);
			this.matchingInsertQuery.bind("pattern", patternId);
			this.matchingInsertQuery.bind("pos", position);
			this.matchingInsertQuery.execute();
			matchingId = (Integer) this.matchingInsertQuery.getReturnedRecord().getValue("id");
		}
		return new ReturnObject(matchingId, created);
	}
	
	private ReturnObject insertMatchingObject(int sentenceId, int patternId, MatchOutputObject object) {
		return this.insertMatching(sentenceId, patternId, object.position);
	}
	
	private int insertPremod(String words, String tags) {
		this.premodSelectQuery.bind("words", words);
		this.premodSelectQuery.bind("tags", tags);
		Integer premodId = (Integer) this.premodSelectQuery.fetchOne("id");
		if (premodId == null) {
			this.premodInsertQuery.bind("words", words);
			this.premodInsertQuery.bind("tags", tags);
			this.premodInsertQuery.execute();
			premodId = (Integer) this.premodInsertQuery.getReturnedRecord().getValue("id");
		}
		return premodId;
	}
	
	private int insertPostmod(String words, String tags) {
		this.postmodSelectQuery.bind("words", words);
		this.postmodSelectQuery.bind("tags", tags);
		Integer postmodId = (Integer) this.postmodSelectQuery.fetchOne("id");
		if (postmodId == null) {
			this.postmodInsertQuery.bind("words", words);
			this.postmodInsertQuery.bind("tags", tags);
			this.postmodInsertQuery.execute();
			postmodId = (Integer) this.postmodInsertQuery.getReturnedRecord().getValue("id");
		}
		return postmodId;
	}
	
	private int insertLemmagroup(String words) {
		this.lemmagroupSelectQuery.bind("words", words);
		Integer lemmagroupId = (Integer) this.lemmagroupSelectQuery.fetchOne("id");
		if (lemmagroupId == null) {
			this.lemmagroupInsertQuery.bind("words", words);
			this.lemmagroupInsertQuery.execute();
			lemmagroupId = (Integer) this.lemmagroupInsertQuery.getReturnedRecord().getValue("id");
		}
		return lemmagroupId;
	}
	
	private int insertNoungroup(String words, String tags, int lemmagroupId) {
		this.noungroupSelectQuery.bind("words", words);
		this.noungroupSelectQuery.bind("tags", tags);
		this.noungroupSelectQuery.bind("lemmas", lemmagroupId);
		Integer noungroupId = (Integer) this.noungroupSelectQuery.fetchOne("id");
		if (noungroupId == null) {
			this.noungroupInsertQuery.bind("words", words);
			this.noungroupInsertQuery.bind("tags", tags);
			this.noungroupInsertQuery.bind("lemmas", lemmagroupId);
			this.noungroupInsertQuery.execute();
			noungroupId = (Integer) this.noungroupInsertQuery.getReturnedRecord().getValue("id");
		}
		return noungroupId;
	}
	
	private int insertTerm(int premodId, int postmodId, int noungroupId) {
		this.termSelectQuery.bind("premod", premodId);
		this.termSelectQuery.bind("postmod", postmodId);
		this.termSelectQuery.bind("noungroup", noungroupId);
		Integer termId = (Integer) this.termSelectQuery.fetchOne("id");
		if (termId == null) {
			this.termInsertQuery.bind("premod", premodId);
			this.termInsertQuery.bind("postmod", postmodId);
			this.termInsertQuery.bind("noungroup", noungroupId);
			this.termInsertQuery.execute();
			termId = (Integer) this.termInsertQuery.getReturnedRecord().getValue("id");
		}
		return termId;
	}
	
	@SuppressWarnings("rawtypes")
	private int insertClassInstance(String type, int matchingId, int termId, int lemmagroupId, int no, int pos, int depth, boolean combined) {
		ResultQuery selectQuery = null;
		InsertQuery insertQuery = null;
		if (type.toUpperCase().equals("I")) {
			selectQuery = this.instanceSelectQuery;
			insertQuery = this.instanceInsertQuery;
		} else if (type.toUpperCase().equals("C")) {
			selectQuery = this.classSelectQuery;
			insertQuery = this.classInsertQuery;
		}
		selectQuery.bind("matching", matchingId);
		selectQuery.bind("term", termId);
		selectQuery.bind("no", no);
		selectQuery.bind("pos", pos);
		selectQuery.bind("depth", depth);
		selectQuery.bind("combined", combined);
		Integer id = (Integer) selectQuery.fetchOne("id");
		if (id == null) {
			insertQuery.bind("matching", matchingId);
			insertQuery.bind("term", termId);
			insertQuery.bind("lemmagroup", lemmagroupId);
			insertQuery.bind("no", no);
			insertQuery.bind("pos", pos);
			insertQuery.bind("depth", depth);
			insertQuery.bind("combined", combined);
			insertQuery.execute();
			id = (Integer) insertQuery.getReturnedRecord().getValue("id");
		}
		return id;
	}
	
	private void insertTermObject(int matchingId, TermOutputObject object) {
		int premodId = insertPremod(object.premod, String.join(":", object.premodTags));
		int postmodId = insertPostmod(object.postmod, String.join(":", object.postmodTags));
		int lemmagroupId = insertLemmagroup(object.lemmas);
		int noungroupId = insertNoungroup(object.nouns, String.join(":", object.nounTags), lemmagroupId);
		int termId = insertTerm(premodId, postmodId, noungroupId);
		this.insertClassInstance(object.type, matchingId, termId, lemmagroupId, object.no, object.position, object.level, object.combined);
	}
	
	private void insertDuplicate(int crawlId, int fileId, DuplicateOutputObject object) {
		int pldId = insertPld(fileId, object.pld);
		int pageId = insertPage(crawlId, fileId, object.url, pldId);
		ReturnObject sentenceReturnObject = insertSentence(object.sentence);
		insertPageSentence(pldId, pageId, sentenceReturnObject.id);
	}
	
}
