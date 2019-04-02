package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.nio.file.Path;

import edu.uci.ics.texera.api.utils.Utils;

public class SQLiteDictionaryManagerConstants {
	
	public static final String TABLE_NAME = "DICTIONARY";
	
	public static final String INDEX_NAME = TABLE_NAME + "_NAME_INDEX";
	
	public static final String DATABASE_FILE_NAME = "TexeraDictionariesDB.db";
	
	public static final Path DICTIONARY_DIR_PATH = Utils.getTexeraHomePath().resolve("user-resources").resolve("dictionaries");
	
	public static final Path DATABASE_FILE_PATH = DICTIONARY_DIR_PATH.resolve(DATABASE_FILE_NAME);
	
	public static final String DICTIONARY_DIR = DATABASE_FILE_PATH.toString();
	
	public static final String SQLITE_CONNECTION_URL = "jdbc:sqlite:" + DICTIONARY_DIR;

}
