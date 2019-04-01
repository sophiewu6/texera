package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
	
	public void addDictionary(String fileName, String dictionaryContent) throws SQLException, JsonProcessingException {
		Connection conn = DriverManager.getConnection(SQLiteDictionaryManagerConstants.SQLITE_CONNECTION_URL);
		DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
		
		List<String> splitedContent = Arrays.asList(dictionaryContent.split(","));
		String serializableDictionaryContent = new ObjectMapper().writeValueAsString(splitedContent);
		
		create.insertInto(DICTIONARY, DICTIONARY.NAME, DICTIONARY.CONTENT)
			.values(fileName, serializableDictionaryContent).execute();
		
		create.close();
		conn.close();
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
	
	public List<String> getDictionary(String dictionaryName) throws SQLException {
		Connection conn = DriverManager.getConnection(SQLiteDictionaryManagerConstants.SQLITE_CONNECTION_URL);
		DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
		
		System.out.println("dictionary name = '" + dictionaryName.trim() + "'");
		ImmutableDictionary dictionary = create.select().from(DICTIONARY)
				.where(DICTIONARY.NAME.eq(dictionaryName.trim())).fetchAny().into(ImmutableDictionary.class);
		
		create.close();
		conn.close();
		return dictionary.getDictionaryEntries();
	}
	
	public static void main(String[] args) throws JsonProcessingException, SQLException {
		SQLiteDictionaryManager.getInstance().addDictionary("sample", "trump, climate, vote");
	}
}
