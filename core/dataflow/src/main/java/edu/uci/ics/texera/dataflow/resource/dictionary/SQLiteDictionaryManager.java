package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import edu.uci.ics.texera.api.exception.StorageException;
import static edu.uci.ics.texera.dataflow.resource.dictionaryJooq.Tables.*;

public class SQLiteDictionaryManager {
	
	private static SQLiteDictionaryManager instance = null;
	
	private SQLiteDictionaryManager() {
		
	}
	
	public synchronized static SQLiteDictionaryManager getInstance() throws SQLException {
		if (instance == null) {
			instance = new SQLiteDictionaryManager();
			instance.createDictionaryManager();
		}
		return instance;
	}
	
	public void createDictionaryManager() throws SQLException {		
		// create directory if not exist
		if (!Files.exists(SQLiteDictionaryManagerConstants.DICTIONARY_DIR_PATH)) {
			try {
				Files.createDirectories(SQLiteDictionaryManagerConstants.DICTIONARY_DIR_PATH);
			} catch (IOException e) {
				throw new StorageException(e);
			}
		}
		
		// create table if not exist
		Connection conn = DriverManager.getConnection(SQLiteDictionaryManagerConstants.SQLITE_CONNECTION_URL);

		DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
		create.createTableIfNotExists(SQLiteDictionaryManagerConstants.TABLE_NAME)
			.column("ID", SQLDataType.INTEGER.identity(true))
			.column("NAME", SQLDataType.VARCHAR.length(128).nullable(false))
			.column("CONTENT", SQLDataType.VARCHAR.length(256).nullable(false))
			.constraints(
					DSL.constraint(SQLiteDictionaryManagerConstants.TABLE_NAME).primaryKey("ID")
			)
			.execute();
		
		create.createIndexIfNotExists(SQLiteDictionaryManagerConstants.INDEX_NAME)
			.on(SQLiteDictionaryManagerConstants.TABLE_NAME, "NAME")
			.execute();
		
		create.close();
		conn.close();
	}
	
	public void destroyDictionaryManager() throws SQLException {
		Connection conn = DriverManager.getConnection(SQLiteDictionaryManagerConstants.SQLITE_CONNECTION_URL);
		DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
		create.dropIndexIfExists(SQLiteDictionaryManagerConstants.INDEX_NAME).execute();
		create.dropTable(SQLiteDictionaryManagerConstants.TABLE_NAME).execute();
		create.close();
		conn.close();
	}
	
	public List<String> addDictionary(String fileName, String dictionaryContent) throws SQLException {
		Connection conn = DriverManager.getConnection(SQLiteDictionaryManagerConstants.SQLITE_CONNECTION_URL);
		DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
		
		create.insertInto(DICTIONARY, DICTIONARY.NAME, DICTIONARY.CONTENT)
			.values(fileName, dictionaryContent).execute();
		
		create.close();
		conn.close();
		
		return null;
	}
	
	public List<String> getDictionaries() throws SQLException {
		Connection conn = DriverManager.getConnection(SQLiteDictionaryManagerConstants.SQLITE_CONNECTION_URL);
		DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
		List<String> dictionaries = create.select(DICTIONARY.NAME)
				.from(DICTIONARY).fetch().into(String.class);
		
		create.close();
		conn.close();
		return dictionaries;
	}
	
	public String getDictionary(String dictionaryName) throws SQLException {
		Connection conn = DriverManager.getConnection(SQLiteDictionaryManagerConstants.SQLITE_CONNECTION_URL);
		DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
		String content = create.select(DICTIONARY.CONTENT).from(DICTIONARY)
				.where(DICTIONARY.NAME.eq(dictionaryName)).fetchAny().into(String.class);
		
		create.close();
		conn.close();
		return content;
	}
	
	public static void main(String[] args ) throws SQLException {
		// Debug:
		
		SQLiteDictionaryManager.getInstance().getDictionaries();
		SQLiteDictionaryManager.getInstance().addDictionary("HenryFIle", "OKOKOK");
		System.out.println(SQLiteDictionaryManager.getInstance().getDictionaries());
		System.out.println(SQLiteDictionaryManager.getInstance().getDictionary("HenryFIle"));
		System.out.println("FINISHES");
		
	}
}
