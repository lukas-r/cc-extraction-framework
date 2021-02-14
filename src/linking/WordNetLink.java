package linking;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.LockingMode;
import org.sqlite.SQLiteConfig.SynchronousMode;
import org.sqlite.SQLiteConfig.TempStore;

import db.DbConnection;
import utils.DbUtils;
import utils.Utils;
import utils.measures.PercentagePrinter;

public class WordNetLink extends DbConnection {
	
	private LinkingConnection connection;
	private DbConnection wordNetDb;
	
	public void findCommonEntities() throws Exception {
		int count = DbUtils.getFirstRow(this.wordNetDb.connection.createStatement().executeQuery("SELECT COUNT(*) FROM word")).getInt(1);
		ResultSet words = this.wordNetDb.connection.createStatement().executeQuery("SELECT key, lemma FROM word");
		PercentagePrinter pp = new PercentagePrinter(count, "entities ", 100, false, false);
		while (words.next()) {
			pp.printAndCount();
			String entityKey = words.getString(1);
			String entityString = words.getString(2);
			Object targetKey = this.connection.findEntity(entityString);
			if (targetKey != null) {
				this.createEntityLink(entityString, entityKey, targetKey);
			}
		}
	}
	
	public void findCommonTuples() throws Exception {
		int count = DbUtils.getFirstRow(super.connection.createStatement().executeQuery("SELECT COUNT(*) FROM entity")).getInt(1);
		PreparedStatement getTuples = this.wordNetDb.connection.prepareStatement("SELECT class_lemma, class_word FROM tuple WHERE instance_word = ?");
		PreparedStatement getTupleEntity = super.connection.prepareStatement("SELECT target_key FROM entity WHERE source_key = ?");
		ResultSet entities = super.connection.createStatement().executeQuery("SELECT entity, source_key, target_key FROM entity");
		PercentagePrinter pp = new PercentagePrinter(count, "tuples ", 100, false, false);
		while (entities.next()) {
			pp.printAndCount();
			String instanceEntityString = entities.getString(1);
			String instanceEntityKeySource = entities.getString(2);
			Object instanceEntityKeyTarget = entities.getObject(3);
			getTuples.setString(1, instanceEntityKeySource);
			ResultSet tuples = getTuples.executeQuery();
			while (tuples.next()) {
				String classEntityString = tuples.getString(1);
				String classEntityKeySource = tuples.getString(2);
				getTupleEntity.setString(1, classEntityKeySource);
				ResultSet classEntityResult = getTupleEntity.executeQuery();
				if (classEntityResult.next()) {
					Object classEntityKeyTarget = classEntityResult.getObject(1);
					Object tupleKeyTarget = this.connection.findTuple(instanceEntityKeyTarget, classEntityKeyTarget);
					if (tupleKeyTarget != null) {
						this.createTupleLink(instanceEntityString, instanceEntityKeySource, instanceEntityKeyTarget, classEntityString, classEntityKeySource, classEntityKeyTarget, 0, tupleKeyTarget);
					}
				}
			}
		}
	}
	
	public void sampleNegativeTuples(double multiplier) throws Exception{
		multiplier = Math.max(multiplier, 1);
		Set<String> usedTuples = new HashSet<String>();
		PreparedStatement getRandomEntity = super.connection.prepareStatement("SELECT entity, target_key FROM entity ORDER BY RANDOM() LIMIT 1;");
		int count = DbUtils.getFirstRow(super.connection.createStatement().executeQuery("SELECT COUNT(*) FROM tuple")).getInt(1);
		int exampleCount = (int) (count * multiplier);
		PreparedStatement getTuple = this.wordNetDb.connection.prepareStatement("SELECT 0 FROM tuple WHERE instance_lemma = ? AND class_lemma = ? LIMIT 1");
		PercentagePrinter pp = new PercentagePrinter(exampleCount, "tuples ", 1, false, true);
		for (int i = 0; i < exampleCount; i++) {
			pp.printAndCount();
			while (true) {
				ResultSet instanceResult = DbUtils.getFirstRow(getRandomEntity.executeQuery());
				String instanceEntityString = instanceResult.getString(1);
				Object instanceEntityKey = instanceResult.getObject(2);
				ResultSet classResult = DbUtils.getFirstRow(getRandomEntity.executeQuery());
				String classEntityString = classResult.getString(1);
				Object classEntityKey = classResult.getObject(2);
				if (!usedTuples.add(instanceEntityString + "_&_" + classEntityString)) {
					continue;
				}
				getTuple.setString(1, instanceEntityString);
				getTuple.setString(2, classEntityString);
				if (!getTuple.executeQuery().next()) {					
					Object tupleKey = this.connection.findTuple(instanceEntityKey, classEntityKey);
					if (tupleKey != null) {
						try {							
							this.createExternalTupleLink(instanceEntityString, instanceEntityKey, classEntityString, classEntityKey, tupleKey, false);
						} catch (Exception e) {
							e.printStackTrace();
						}
						break;
					}
				}
			}
		}
	}
	
	public void findAllTuplesWithCrossProduct() throws Exception {
		String getEntitiesQueryString = "SELECT DISTINCT entity, target_key FROM entity";
		int count = DbUtils.getFirstRow(super.connection.createStatement().executeQuery("SELECT COUNT(DISTINCT target_key) FROM entity")).getInt(1);
		PreparedStatement getTuple = this.wordNetDb.connection.prepareStatement("SELECT 0 FROM tuple WHERE instance_lemma = ? AND class_lemma = ? LIMIT 1");
		ResultSet instanceEntities = super.connection.createStatement().executeQuery(getEntitiesQueryString);
		PercentagePrinter pp = new PercentagePrinter((long) count * count, "tuples ", 10, false, true);
		while (instanceEntities.next()) {
			String instanceEntityString = instanceEntities.getString(1);
			Object instanceEntityKeyTarget = instanceEntities.getObject(2);
			ResultSet classEntities = super.connection.createStatement().executeQuery(getEntitiesQueryString);
			while (classEntities.next()) {
				pp.printAndCount();
				String classEntityString = classEntities.getString(1);
				Object classEntityKeyTarget = classEntities.getObject(2);
				Object tupleKeyTarget = this.connection.findTuple(instanceEntityKeyTarget, classEntityKeyTarget);
				if (tupleKeyTarget != null) {
					getTuple.setObject(1, instanceEntityString);
					getTuple.setObject(2, classEntityString);
					ResultSet tuple = getTuple.executeQuery();
					boolean found = tuple.next();
					this.createExternalTupleLink(instanceEntityString, instanceEntityKeyTarget, classEntityString, classEntityKeyTarget, tupleKeyTarget, found);
				}
			}
		}
	}
	
	public void findAllTuplesByIterating() throws Exception {
		PreparedStatement findTuples = this.wordNetDb.connection.prepareStatement("SELECT 0 FROM tuple WHERE instance_lemma = ? AND class_lemma = ? LIMIT 1");
		PreparedStatement findEntity = super.connection.prepareStatement("SELECT 0 FROM entity WHERE target_key = ? LIMIT 1");
		PercentagePrinter pp = new PercentagePrinter(this.connection.tupleCount(), "tuples ", 1000, false, true);
		String previousInstance = "";
		boolean previousInstanceFound = false;
		String previousClass = "";
		boolean previousClassFound = false;
		for (Pair<Object, Pair<Pair<Object, String>, Pair<Object, String>>> tuple: this.connection.tuples()) {
			pp.printAndCount();
			Object tupleId = tuple.getLeft();
			String instanceEntityString = tuple.getRight().getLeft().getRight();
			Object instanceEntityKey = tuple.getRight().getLeft().getLeft();
			if (!instanceEntityKey.equals(previousInstance)) {
				findEntity.setObject(1, instanceEntityKey);
				previousInstanceFound = findEntity.executeQuery().next();
				previousInstance = instanceEntityString;
			}
			String classEntityString = tuple.getRight().getRight().getRight();
			Object classEntityKey = tuple.getRight().getRight().getLeft();
			if (!classEntityString.equals(previousClass)) {
				findEntity.setObject(1, classEntityKey);
				previousClassFound = findEntity.executeQuery().next();
				previousClass = classEntityString;
			}
			if (previousInstanceFound && previousClassFound) {
				findTuples.setString(1, instanceEntityString);
				findTuples.setString(2, classEntityString);
				boolean found = findTuples.executeQuery().next();
				this.createExternalTupleLink(instanceEntityString, instanceEntityKey, classEntityString, classEntityKey, tupleId, found);
			}
		}
	}
	
	public void link() throws Exception {
		this.findCommonTuples();
		this.findCommonEntities();
		this.findAllTuplesWithCrossProduct();
		this.connection.close();
	}
	
	@Override
	protected SQLiteConfig createConfig(boolean readOnly) {
		SQLiteConfig config = super.createConfig(readOnly);
		config.setLockingMode(LockingMode.NORMAL);
		config.setSynchronous(SynchronousMode.FULL);
		config.setTempStore(TempStore.FILE);
		return config;
	}
	
	public WordNetLink(File wordNetDbFile, File linkingDbFile, LinkingConnection connection) {
		super(linkingDbFile, false);
		this.connection = connection;
		this.wordNetDb = new DbConnection(wordNetDbFile, true) {};
	}

	public static void main(String[] args) {
		System.out.println(PercentagePrinter.df.format(LocalDateTime.now()));
		System.out.println();
		try {
			WordNetLink link = new WordNetLink(new File("C:\\Users\\Lukas\\Desktop\\work\\wordnet\\wordnet.sqlite"), new File("C:\\Users\\Lukas\\Desktop\\work\\wordnet\\linking_webisa.sqlite"), new WebIsAConnection("localhost", 27017, "testdb", 524672110));
			//WordNetLink link = new WordNetLink(new File("C:\\Users\\Lukas\\Desktop\\work\\wordnet\\wordnet.sqlite"), new File("C:\\Users\\Lukas\\Desktop\\work\\wordnet\\linking_lemma.sqlite"), new CommonDbConnection(new File("C:\\Users\\Lukas\\Desktop\\work\\wordnet\\db.sqlite"), new File("C:\\Users\\Lukas\\Desktop\\work\\wordnet\\relations.sqlite"), PairType.LEMMA));
			//link.findCommonEntities();
			System.out.println();
			//link.findAllTuplesWithCrossProduct();
			//link.findAllTuplesByIterating();
			link.sampleNegativeTuples(1);
			//link.writeCSV(new File("C:\\Users\\Lukas\\Desktop\\work\\wordnet\\webisa_sampled.csv.gzip"), true);
			link.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("static-access")
	public void writeCSV(File csv, boolean includeTupleTable) throws Exception {
		final String delimiter = ",";
		long count = DbUtils.getFirstRow(super.connection.createStatement().executeQuery("SELECT count(*) FROM external_tuple")).getLong(1);
		if (includeTupleTable) {

			count += DbUtils.getFirstRow(super.connection.createStatement().executeQuery("SELECT count(*) FROM tuple")).getLong(1);
		}
		PercentagePrinter pp = new PercentagePrinter(count, "tuples ", 10, false, true);
		Writer output = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(csv)))));
		Consumer<String[]> writeLine = (columns) -> {
			try {
				output.write(Stream.of(columns).map(StringEscapeUtils::escapeCsv).collect(Collectors.joining(delimiter)));
				output.write(System.lineSeparator());
			} catch (IOException e) {
				e.printStackTrace();
			}
		};
		ResultSet tuples = super.connection.createStatement().executeQuery("SELECT instance_entity, class_entity, instance_entity_key, class_entity_key, tuple_key, found FROM external_tuple");
		writeLine.accept(ArrayUtils.add(this.connection.ATTRIBUTES, "correct"));
		while (tuples.next()) {
			pp.printAndCount();
			String instanceString = tuples.getString(1);
			String classString = tuples.getString(2);
			Object instanceKey = tuples.getObject(3);
			Object classKey = tuples.getObject(4);
			Object tuplesKey = tuples.getObject(5);
			boolean found = tuples.getBoolean(6);
			String[] features = ArrayUtils.add(this.connection.getTupleAttributes(instanceString, classString, instanceKey, classKey, tuplesKey), Integer.valueOf(Utils.intFromBool(found)).toString());
			writeLine.accept(features);
		}
		if (includeTupleTable) {
			System.out.println("\n\n---TUPLE TABLE---\n");
			tuples = super.connection.createStatement().executeQuery("SELECT instance_entity, class_entity, instance_target_entity_key, class_target_entity_key, tuple_target_key FROM tuple");
			while (tuples.next()) {
				pp.printAndCount();
				String instanceString = tuples.getString(1);
				String classString = tuples.getString(2);
				Object instanceKey = tuples.getObject(3);
				Object classKey = tuples.getObject(4);
				Object tuplesKey = tuples.getObject(5);
				String[] features = ArrayUtils.add(this.connection.getTupleAttributes(instanceString, classString, instanceKey, classKey, tuplesKey), Integer.valueOf(Utils.intFromBool(true)).toString());
				writeLine.accept(features);
			}
		}
		output.close();
	}
	
	@Override
	protected void createTables() {
		this.context.createTable("entity")
			.column("entity", SQLDataType.VARCHAR)
			.column("source_key", SQLDataType.VARCHAR)
			.column("target_key", SQLDataType.INTEGER)
			.constraints(
				DSL.unique("source_key", "target_key")
			)
			.execute();
		this.context.createIndex().on("entity", "entity");
		this.context.createIndex().on("entity", "source_key");
		this.context.createIndex().on("entity", "entity", "target_key");
		this.context.createIndex().on("entity", "target_key");
		
		this.context.createTable("tuple")
			.column("instance_entity", SQLDataType.VARCHAR)
			.column("class_entity", SQLDataType.VARCHAR)
			.column("instance_source_entity_key", SQLDataType.VARCHAR)
			.column("instance_target_entity_key", SQLDataType.VARCHAR)
			.column("class_source_entity_key", SQLDataType.VARCHAR)
			.column("class_target_entity_key", SQLDataType.VARCHAR)
			.column("tuple_source_key", SQLDataType.VARCHAR)
			.column("tuple_target_key", SQLDataType.VARCHAR)
			.constraints(
				DSL.unique("instance_source_entity_key", "class_source_entity_key", "instance_target_entity_key", "class_target_entity_key")
			)
			.execute();
		this.context.createIndex().on("tuple", "instance_entity");
		this.context.createIndex().on("tuple", "class_entity");
		this.context.createIndex().on("tuple", "instance_source_entity_key");
		this.context.createIndex().on("tuple", "instance_target_entity_key");
		this.context.createIndex().on("tuple", "class_source_entity_key");
		this.context.createIndex().on("tuple", "class_target_entity_key");
		this.context.createIndex().on("tuple", "tuple_source_key");
		this.context.createIndex().on("tuple", "tuple_target_key");
		
		this.context.createTable("external_tuple")
			.column("instance_entity", SQLDataType.VARCHAR)
			.column("class_entity", SQLDataType.VARCHAR)
			.column("instance_entity_key", SQLDataType.VARCHAR)
			.column("class_entity_key", SQLDataType.VARCHAR)
			.column("tuple_key", SQLDataType.VARCHAR)
			.column("found", SQLDataType.BOOLEAN)
			.constraints(
				DSL.unique("instance_entity_key", "class_entity_key")
			)
			.execute();
		this.context.createIndex().on("tuple", "instance_entity");
		this.context.createIndex().on("tuple", "class_entity");
		this.context.createIndex().on("tuple", "instance_entity_key");
		this.context.createIndex().on("tuple", "class_entity_key");
		this.context.createIndex().on("tuple", "tuple_key");
	}
	
	private void createEntityLink(String entity, String sourceKey, Object targetKey) throws SQLException {
		PreparedStatement stmt = super.connection.prepareStatement("INSERT INTO entity (entity, source_key, target_key) VALUES (?, ?, ?)");
		stmt.setString(1, entity);
		stmt.setString(2, sourceKey);
		stmt.setObject(3, targetKey);
		stmt.execute();
	}
	
	private void createTupleLink(String instanceEntity, String instanceSourceKey, Object instanceTargetKey, String classEntity, String classSourceKey, Object classTargetKey, int sourceTupleKey, Object targetTupleKey) throws SQLException {
		PreparedStatement stmt = super.connection.prepareStatement("INSERT INTO tuple (instance_entity, instance_source_entity_key, instance_target_entity_key, class_entity, class_source_entity_key, class_target_entity_key, tuple_source_key, tuple_target_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
		stmt.setString(1, instanceEntity);
		stmt.setString(2, instanceSourceKey);
		stmt.setObject(3, instanceTargetKey);
		stmt.setString(4, classEntity);
		stmt.setString(5, classSourceKey);
		stmt.setObject(6, classTargetKey);
		stmt.setInt(7, sourceTupleKey);
		stmt.setObject(8, targetTupleKey);
		stmt.execute();
	}
	
	private void createExternalTupleLink(String instanceEntity, Object instanceKey, String classEntity, Object classKey, Object tupleKey, boolean found) throws SQLException {
		PreparedStatement stmt = super.connection.prepareStatement("INSERT INTO external_tuple (instance_entity, instance_entity_key, class_entity, class_entity_key, tuple_key, found) VALUES (?, ?, ?, ?, ?, ?)");
		stmt.setString(1, instanceEntity);
		stmt.setObject(2, instanceKey);
		stmt.setString(3, classEntity);
		stmt.setObject(4, classKey);
		stmt.setObject(5, tupleKey);
		stmt.setBoolean(6, found);
		stmt.execute();
	}

}
