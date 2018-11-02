package edu.uci.ics.texera.web.resource.metadata;

import java.util.List;

import static edu.uci.ics.texera.storage.constants.MySQLConstants.*;

import java.sql.Connection;
import java.sql.DriverManager;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.Record;

import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;

import edu.uci.ics.texera.web.resource.metadata.tables.records.*;
import edu.uci.ics.texera.web.resource.metadata.Tables;

public class MetadataResource {

    private static MetadataResource instance = null;
    private static DSLContext create = null;
    private MetadataResource() throws StorageException {
        // establish JDBC connection to Database
        String url = null;
        Connection connection = null;
        try {
            Class.forName(driver).newInstance();
            url = "jdbc:mysql://" + HOST + ":" + PORT + "/"
                    + DATABASE + "?autoReconnect=true&useSSL=true";
            connection = DriverManager.getConnection(url, USERNAME, PASSWORD);
        } catch (Exception e) {
            System.out.println("Could not get connection with " + url + e.getMessage());
        }
        // get instance of DSLContext
        try {
            create = DSL.using(connection, SQLDialect.MYSQL);
        } catch (Exception e) {
            System.out.println("Could not get DSLContext with mysql connection established" + e.getMessage());
        }
    }


    public synchronized static MetadataResource getInstance() throws StorageException {
        if (instance == null) {
            instance = new MetadataResource();
        }
        return instance;
    }

    /**
     * add a record into dictionary table
     * @param fileName
     * @param dictionaryContent
     */
    public void addDictionary(String fileName, String dictionaryContent) throws StorageException {
        try {
            DictionaryRecord dict = new DictionaryRecord(fileName, fileName, dictionaryContent);
            create.executeInsert(dict);
        } catch (Exception e) {
            throw new TexeraException(
                    "MetadataResource failed to insert record " + fileName + " into table Dictionary " + e.getMessage());
        }
    }

    /**
     * get the names of all records in dictionary table
     * @return list of all names
     */
    public List<DictionaryRecord> getDictionaries() throws StorageException {
        List<DictionaryRecord> dictionaries = null;
        try {
            dictionaries = create.select()
                                 .from(Tables.DICTIONARY)
                                 .fetchInto(DictionaryRecord.class);
        } catch (Exception e) {
            throw new StorageException(
                    "MetadataResource failed to get all dictionaries. " + e.getMessage());
        }
        return dictionaries;
    }
    /**
     * get the content of a record with specified name
     * @param dictionaryName
     * @return the content of that record
     */
    public DictionaryRecord getDictionary(String dictionaryName) throws StorageException {
        List<DictionaryRecord> dictionaries = null;
        try {
            dictionaries = create.select()
                                 .from(Tables.DICTIONARY)
                                 .where(Tables.DICTIONARY.ID.eq(dictionaryName))
                                 .fetchInto(DictionaryRecord.class);
            if (dictionaries.size() == 1)
                return dictionaries.get(0);
        } catch (Exception e) {
            throw new StorageException(
                    "MetadataResource failed to get dictionary with id&name == " + dictionaryName + ". " + e.getMessage());
        }
        return null;
    }
}