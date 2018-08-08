package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.storage.utils.StorageUtils;
import edu.uci.ics.texera.storage.constants.MySQLConstants;

public class DictionaryManager {

    private static DictionaryManager instance = null;
    private static Connection connection;
    private static Statement statement;
    private static PreparedStatement prepStatement;

    private DictionaryManager() throws StorageException {
        // Establish JDBC connection to MySQL
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String url = "jdbc:mysql://" + MySQLConstants.HOST + ":" + MySQLConstants.PORT + "/"
                    + MySQLConstants.DATABASE + "?autoReconnect=true&useSSL=true";
            connection = DriverManager.getConnection(url, MySQLConstants.USERNAME, MySQLConstants.PASSWORD);
            statement = connection.createStatement();
        } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new DataflowException("DictionaryManager failed to connect to mysql database." + e.getMessage());
        }
    }


    public synchronized static DictionaryManager getInstance() throws StorageException {
        if (instance == null) {
            instance = new DictionaryManager();
            instance.createDictionaryManager();
        }
        return instance;
    }

    /**
     * Creates plan store, both an index and a directory for plan objects.
     *
     * @throws TexeraException
     */
    public void createDictionaryManager() throws TexeraException {
        // table should not exist
        if (checkTableExistence(DictionaryManagerConstants.TABLE_NAME)) {
            throw new StorageException(String.format("Table %s already exists.", DictionaryManagerConstants.TABLE_NAME));
        }

        // create the table
        List<Attribute> attributeList = DictionaryManagerConstants.SCHEMA.getAttributes();
        String createTableStatement = "CREATE TABLE " + DictionaryManagerConstants.TABLE_NAME + " (\n";
        createTableStatement += attributeList.stream().map(attr -> convertAttribute(attr))
                .collect(Collectors.joining(",\n"));
        createTableStatement += "\n); ";
        try {
            if (statement == null)
                statement = connection.createStatement();
            statement.executeUpdate(createTableStatement);
        } catch (SQLException e) {
            throw new DataflowException(
                    "DictionaryManager failed to create table " + DictionaryManagerConstants.TABLE_NAME + ". " + e.getMessage());
        }
    }

    /**
     * removes plan store, both an index and a directory for dictionary objects.
     *
     * @throws TexeraException
     */
    public void destroyDictionaryManager() throws TexeraException {
        // table should exist
        if (!checkTableExistence(DictionaryManagerConstants.TABLE_NAME)) {
            throw new StorageException(String.format("Table %s does not exist.", DictionaryManagerConstants.TABLE_NAME));
        }
        // delete the table
        String dropTableStatement = "DROP TABLE " + DictionaryManagerConstants.TABLE_NAME + ";";
        try {
            if (statement == null)
                statement = connection.createStatement();
            statement.executeUpdate(dropTableStatement);
        } catch (SQLException e) {
            throw new DataflowException(
                    "DictionaryManager failed to delete table " + DictionaryManagerConstants.TABLE_NAME + ". " + e.getMessage());
        }

        StorageUtils.deleteDirectory(DictionaryManagerConstants.DICTIONARY_DIR);
    }

    public void addDictionary(String fileName, String dictionaryContent) throws StorageException {
        String insertStatement = "INSERT INTO " + DictionaryManagerConstants.TABLE_NAME + " VALUES(?, ?);";
        try {
            if (prepStatement == null)
                prepStatement = connection.prepareStatement(insertStatement);
            prepStatement.setString(1, fileName);
            prepStatement.setString(2, dictionaryContent);
            prepStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DataflowException(
                    "DictionaryManager failed to insert row " + fileName + " into table " + DictionaryManagerConstants.TABLE_NAME + ". " + e.getMessage());
        }
    }

    public List<String> getDictionaries() throws StorageException {
        List<String> dictionaries = new ArrayList<>();
        String getAllStatement = "SELECT * FROM " + DictionaryManagerConstants.TABLE_NAME + ";";
        try {
            if (statement == null)
                statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(getAllStatement);
            while (rs.next()) {
                dictionaries.add(rs.getString(DictionaryManagerConstants.NAME));
            }
        } catch (SQLException e) {
            throw new StorageException(
                    "DictionaryManager failed to get all dictionaries. " + e.getMessage());
        }
        return dictionaries;
    }

    public String getDictionary(String dictionaryName) throws StorageException {
        String getOneStatement = "SELECT * FROM " + DictionaryManagerConstants.TABLE_NAME + " WHERE "
                + DictionaryManagerConstants.NAME + "='" + dictionaryName + "';";
        try {
            if (statement == null)
                statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(getOneStatement);
            if (rs.next())
                return rs.getString(DictionaryManagerConstants.CONTENT);
        } catch (SQLException e) {
            throw new StorageException(
                    "DictionaryManager failed get : " + dictionaryName + ". " + e.getMessage());
        }
        return null;
    }

    public boolean checkTableExistence(String tableName) throws StorageException {
        String checkStatement = "SHOW TABLES LIKE '" + tableName + "';";
        try {
            if (statement == null)
                statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(checkStatement);
            return rs.next();
        } catch (SQLException e) {
            throw new StorageException(
                    "DictionaryManager failed to check table existence of: " + tableName + ". " + e.getMessage());
        }
    }

    /**
     *
     * Convert a texera attribute into one line of sql statement. Texera
     * attribute is from outputSchema. Used in the create table statement.
     *
     * @param attribute
     * @return
     */
    private String convertAttribute(Attribute attribute) {
        String sqlAttrTypeName = attribute.getType().getName();
        String sqlStatement = "\t" + attribute.getName();
        switch (sqlAttrTypeName) {
            case "integer":
                sqlStatement += " INT";
                break;
            case "double":
                sqlStatement += " DOUBLE";
                break;
            case "date":
                sqlStatement += " DATE";
                break;
            default:
                sqlStatement += " TEXT";
                break; // Including string and text
        }
        return sqlStatement;
    }
}