package edu.uci.ics.texera.web.resource.metadata;

import java.util.List;
import java.util.Arrays;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.storage.constants.MySQLConstants;


public class MetadataResource
{
    private static MetadataResource instance = null;
    private static Jdbi jdbi = null;
    private static Handle handle = null;
    private static DictionaryDAO dictionaryDAO;

    private MetadataResource() throws StorageException
    {
        // establish JDBI connection to MySQL attached to handle
        MysqlDataSource ds = new MysqlDataSource();
        ds.setServerName(MySQLConstants.HOST);
        ds.setPortNumber(Integer.parseInt(MySQLConstants.PORT));
        ds.setDatabaseName(MySQLConstants.DATABASE);
        ds.setUser(MySQLConstants.USERNAME);
        ds.setPassword(MySQLConstants.PASSWORD);

        jdbi = Jdbi.create(ds)
                   .installPlugin(new SqlObjectPlugin());
        handle = jdbi.open();

        //generate DAO object from JDBI
        dictionaryDAO = handle.attach(DictionaryDAO.class);
    }


    public synchronized static MetadataResource getInstance() throws StorageException {
        if (instance == null)
        {
            instance = new MetadataResource();
            instance.createMetadataResource();
        }
        return instance;
    }

    /**
     * Create tables for metadata
     * @throws StorageException
     */
    public void createMetadataResource() throws TexeraException
    {
        dictionaryDAO.createTable();
    }

    /**
     * Add one record into dictionary table
     */
    public void addDictionary(String fileName, List<String> dictionaryContent) throws StorageException
    {
        dictionaryDAO.insertDictionary(new Dictionary(fileName, fileName, dictionaryContent));
    }

    /**
     * get all dictionaries as a list of Dictionary obejcts
     */
    public List<Dictionary> getDictionaries() throws StorageException {
        return dictionaryDAO.listAllDictionaries();
    }

    /**
     * get the dictionary with given name as a Dictionary obejct
     */
    public Dictionary getDictionary(String dictionaryName) throws StorageException {
        return dictionaryDAO.getDictionaryByID(dictionaryName);
    }

    public boolean checkTableExistence(String tableName) throws StorageException {
        List<String> result = jdbi.withHandle(handle ->
            handle.createQuery("SHOW TABLES LIKE '" + tableName + "';")
                  .mapTo(String.class)
                  .list());
        return result.contains(tableName);
    }
}
