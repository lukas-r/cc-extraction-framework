package db;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.LockingMode;
import org.sqlite.SQLiteConfig.SynchronousMode;
import org.sqlite.SQLiteConfig.TempStore;

public abstract class DbConnection {
	
	private final static String CONNECTION_PREFIX = "jdbc:sqlite:";
	
	public File dbFile;
	public Connection connection;
	public DSLContext context;
	
	static {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static SQLiteConfig getDefaultConfig(boolean readOnly) {
		SQLiteConfig config = new SQLiteConfig();
		config.setPageSize(4096);
		config.setCacheSize(10000);
		config.setLockingMode(LockingMode.EXCLUSIVE);
		config.setSynchronous(SynchronousMode.OFF);
		config.setJournalMode(JournalMode.WAL);
		config.setTempStore(TempStore.MEMORY);
		config.enforceForeignKeys(false);
		config.setReadOnly(readOnly);
		return config;
	}
	
	protected SQLiteConfig createConfig(boolean readOnly) {
		return getDefaultConfig(readOnly);
	}
	
	public DbConnection(File dbFile, boolean readOnly) {
		this.dbFile = dbFile;
		boolean newDb = !dbFile.exists();
		SQLiteConfig config = this.createConfig(readOnly);
		try {
			this.connection = config.createConnection(CONNECTION_PREFIX + dbFile.getAbsolutePath());
			this.context = DSL.using(this.connection, SQLDialect.SQLITE);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if (newDb) {
			this.createTables();
			this.initialPrepare();
		}
		this.prepare();
	}
	
	public DbConnection(File dbFile) {
		this(dbFile, false);
	}
	
	public void close() {
		try {
			this.context.close();
			this.connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	protected void createTables() {}
	
	protected void initialPrepare() {}
	
	protected void prepare() {}

}
