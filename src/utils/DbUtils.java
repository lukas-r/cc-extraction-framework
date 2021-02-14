package utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class DbUtils {
	
	public static ResultSet getFirstRow(ResultSet results) throws SQLException {
		results.next();
		return results;
	}
		
	public static synchronized int getInsertedKey(Statement statement) throws SQLException {
		return DbUtils.getFirstRow(statement.getGeneratedKeys()).getInt(1);
	}

}
