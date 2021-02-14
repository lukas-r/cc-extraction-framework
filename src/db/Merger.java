package db;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.InsertQuery;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.google.common.io.Files;

public class Merger {
	
	private final boolean insert;
	
	private File firstDb;
	private File secondDb;
	private File mergedDb;
	private File tmpDir;
	
	private MergeConnection targetConnection;
	private MergeConnection sourceConnection;
	
	public Merger(File firstDb, File secondDb, File mergedDb, File tmpDir) {
		this.insert = false;
		this.firstDb = firstDb;
		this.secondDb = secondDb;
		this.mergedDb = mergedDb;
		if (tmpDir == null) {
			this.tmpDir = new File(System.getProperty("java.io.tmpdir"));
		} else {
			this.tmpDir = tmpDir;
		}
	}
	
	public Merger(File sourceDb, File targetDb, File tmpDir) {
		this.insert = true;
		this.firstDb = sourceDb;
		this.secondDb = targetDb;
	}
	
	public void merge() {
		try {
				if (insert) {
					this.sourceConnection = new MergeConnection(this.firstDb, false);
					this.targetConnection = new MergeConnection(secondDb, false);
				} else {
					System.out.println("Copy target DB");
					Files.copy(firstDb, mergedDb);
					File tmpDb = File.createTempFile("db_translate_", ".sqlite", tmpDir);
					tmpDb.deleteOnExit();
					System.out.println("Copy source DB");
					Files.copy(secondDb, tmpDb);
					System.out.println("Copy done");
					this.sourceConnection = new MergeConnection(tmpDb, false);
					this.targetConnection = new MergeConnection(this.mergedDb, false);
				}
				List<TableTranslation> translations = Arrays.asList(
						new TableTranslation("crawl", "name").addTableColumns("file", "page"),
						new TableTranslation("file", "crawl", "file").addTableColumns("page"),
						new TableTranslation("pld", "pld").addTableColumns("page", "page_sentence"),
						new TableTranslation("page", "file", "url").addTableColumns("page_sentence"),
						new TableTranslation("sentence", "sentence").addTableColumns("page_sentence", "matching"),
						new TableTranslation("page_sentence", "pld", "sentence"),
						new TableTranslation("pattern", "name").addTableColumns("matching"),
						new TableTranslation("matching", "sentence", "pattern", "pos").addTableColumns("matching_info", "instance", "class"),
						new TableTranslation("matching_info", "matching"),
						new TableTranslation("lemmagroup", "words").addTableColumns(new TableColumn("noungroup", "lemmas")),
						new TableTranslation("premod", "words", "tags").addTableColumns("term"),
						new TableTranslation("postmod", "words", "tags").addTableColumns("term"),
						new TableTranslation("noungroup", "words", "tags", "lemmas").addTableColumns("term"),
						new TableTranslation("term", "premod", "postmod", "noungroup").addTableColumns("instance", "class"),
						new TableTranslation("instance", "matching", "term", "no", "pos", "depth", "combinded"),
						new TableTranslation("class", "matching", "term", "no", "pos", "depth", "combinded")
				);
				System.out.println("Merge");
				for (TableTranslation translation: translations) {
					this.mergeTable(translation);
				}
				/*System.out.println("Copy");
				for (TableTranslation translation: translations) {
					this.copyTable(translation.table);
				}*/
				System.out.println("done");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}
	
	private Field<?>[] fieldsWithoutId(Record record) {
		return Arrays.stream(record.fields()).filter(f -> !f.getName().equals("id")).toArray(Field[]::new);
	}
	
	private List<Condition> recordToConditions(Record record, List<String> columns) {
		return Arrays.stream(record.fields()).filter(f -> columns.contains(f.getName())).map(f -> DSL.field(DSL.name(f.getName())).eq(f.getValue(record))).collect(Collectors.toList());
	}
	
	private void mergeTable(TableTranslation translation) {
		try {
			Map<Integer, Integer> map = new HashMap<Integer, Integer>();
			Table<Record> table = DSL.table(DSL.name(translation.table));
			
			Record maxSourceIdRecord = this.sourceConnection.context.fetchOne("SELECT MAX(id) FROM `" + translation.table + "`");
			int maxSourceId = maxSourceIdRecord == null ? 0 : (Integer) maxSourceIdRecord.get(0);
			int i = 0;
			
			boolean allIdsEqual = true;
			for (Record record: this.sourceConnection.context.selectQuery(table)) {
				if (i % 10000 == 0) {
					System.out.println("Merge insert table " + translation.table + "\t" + i + "\t\tfrom " + maxSourceId);
					this.targetConnection.connection.commit();
				}
				i++;
				
				Field<Object> fieldId = DSL.field(DSL.name("id"));
				int oldId = ((Integer) record.get("id")).intValue();
				record.detach();
				record = record.into(fieldsWithoutId(record));
				record.changed(true);
				InsertQuery<?> query = (InsertQuery<?>) this.targetConnection.context.insertInto(table).set(record).onDuplicateKeyIgnore().returning(fieldId).keepStatement(false);
				Record result = query.execute() == 0 ? null : query.getReturnedRecord();
				if (result == null) {
					result = this.targetConnection.context.select(fieldId).from(table).where(recordToConditions(record, translation.columns)).fetchOne();
				}
				Integer newId = (Integer) result.get("id");
				map.put(oldId, newId);
				if (allIdsEqual && newId != oldId) {
					allIdsEqual = false;
				}
			}
			this.targetConnection.connection.commit();
			
			if (!allIdsEqual) {
				Record maxTargetIdRecord = this.targetConnection.context.fetchOne("SELECT MAX(id) FROM `" + translation.table + "`");
				int maxTargetId = maxTargetIdRecord == null ? 0 : (Integer) maxTargetIdRecord.get(0);
				
				for (TableColumn column: translation.tableColumns) {
					PreparedStatement updateStatement = this.sourceConnection.connection.prepareStatement("UPDATE `" + column.table + "` SET `" + column.column + "` = ? WHERE `" + column.column + "` = ?");
					i = 0;
					for (Entry<Integer, Integer> entry: map.entrySet()) {
						if (i % 10000 == 0) {
							System.out.println("Merge id table " + translation.table + " in " + column.table + "\t" + i + "\t\tfrom " + maxTargetId);
							this.sourceConnection.connection.commit();
						}
						i++;
						updateStatement.setInt(1, entry.getValue() + maxTargetId);
						updateStatement.setInt(2, entry.getKey());
						updateStatement.execute();
					}
					updateStatement.close();
					if (maxTargetId > 0) {
						this.sourceConnection.connection.createStatement().execute("UPDATE `" + column.table + "` SET `" + column.column + "` = `" + column.column + "` - " + maxTargetId);
					}
				}
			}
			this.sourceConnection.connection.commit();
		} catch (Exception e) {
			e.printStackTrace();
			try {
				this.sourceConnection.connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			try {
				this.sourceConnection.connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings("unused")
	private void copyTable(String tableName) {
		Table<Record> table = DSL.table(DSL.name(tableName));
		
		Record maxIdRecord = this.sourceConnection.context.fetchOne("SELECT MAX(id) FROM `" + tableName + "`");
		int maxId = maxIdRecord == null ? 0 : (Integer) maxIdRecord.get(0);
		int i = 0;
		
		for (Record record: this.sourceConnection.context.fetch("SELECT * FROM `" + tableName + "`")) {
			if (i % 10000 == 0) {
				System.out.println("Copy table " + tableName + " " + i + "\t\tfrom " + maxId);
			}
			record.detach();
			record = record.into(fieldsWithoutId(record));
			record.changed(true);
			this.targetConnection.context.insertInto(table).set(record).onDuplicateKeyIgnore().execute();
		}
	}
	
	private void close() {
		this.sourceConnection.close();
		this.targetConnection.close();
	}
	
	public static class TableTranslation {
		
		public final String table;
		public final List<String> columns;
		public final List<TableColumn> tableColumns;
		
		public TableTranslation(String table, String ...columns) {
			this.table = table;
			this.columns = Arrays.asList(columns);
			this.tableColumns = new ArrayList<TableColumn>();
		}
		
		public TableTranslation addTableColumns(String ...tables) {
			for (String table: tables) {
				this.tableColumns.add(new TableColumn(table, this.table));
			}
			return this;
		}
		
		public TableTranslation addTableColumns(TableColumn ...columns) {
			for (TableColumn column: columns) {
				this.tableColumns.add(column);
			}
			return this;
		}
		
	}
	
	public static class TableColumn {
		
		public final String table;
		public final String column;
		
		public TableColumn(String table, String column) {
			this.table = table;
			this.column = column;
		}
		
	}
	
	private static class MergeConnection extends DbConnection {

		public MergeConnection(File dbFile, boolean readOnly) {
			super(dbFile, readOnly);
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
