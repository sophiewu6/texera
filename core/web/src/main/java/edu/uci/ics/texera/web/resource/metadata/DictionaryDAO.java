package edu.uci.ics.texera.web.resource.metadata;


import org.jdbi.v3.core.mapper.RowMapper;
 import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
 import org.jdbi.v3.sqlobject.customizer.Bind;
 import org.jdbi.v3.sqlobject.customizer.BindMethods;
 import org.jdbi.v3.sqlobject.statement.SqlQuery;
 import org.jdbi.v3.sqlobject.statement.SqlUpdate;

 import java.sql.ResultSet;
 import java.sql.SQLException;
 import java.util.List;

 /**
  * DictionaryDAO communicates with database for User-Dictionary related tasks.
  * DictionaryDAO provides:
  *  - create tables
  *  - query dictionaries by ID or by a specific user
  *  - insert / update / delete dictionaries
  *
  * --Note: for this sample, look at UserDAO first to see explanation of a DAO object
  * --Note: the DAO class would replace the old DictionaryManager class, which does similar encapsulation
  *
  * @author Zuozhi Wang
  * Created at 10/21/2018
  */
 public interface DictionaryDAO {

     /**
      * A Custom Row Mapper that maps a result row to a Dictionary Object.
      * The mapper de-serializes the dictionary content in String format stored in DB
      * back to a list of Strings.
      * <p>
      * --Note: The reason that we have to provide a custom row mapper is
      * --      because of the conversion between a list and a string.
      * --      Normally, JDBI can perform the mapping automatically in most cases.
      */
     class DictionaryMapper implements RowMapper<Dictionary> {
         @Override
         public Dictionary map(ResultSet rs, StatementContext ctx) throws SQLException {
             return new Dictionary(rs.getString("id"), rs.getString("name"),
                     Dictionary.fromContentString(rs.getString("content")));
         }
     }

     /**
      * Creates the Dictionary table with
      * id as primary key, dictionary name, and content stored in String format.
      * <p>
      * A dictionary is always associated with a user. The Dictionary-User connection table
      * DictionaryOfUser is also created with foreign keys userID and dictID.
      * <p>
      * Dictionary content is a list of String. The list serialized to a String when storing in DB,
      * and is de-serialized back into a list of Strings when mapped to a Dictionary object.
      */
     @SqlUpdate("CREATE TABLE IF NOT EXISTS Dictionary (id VARCHAR(50) PRIMARY KEY, name TEXT, content TEXT);")
     void createTable();

     /**
      * Adds a Dictionary into the database with its corresponding User.
      * The Dictionary ID must be unique (such as a random UUID).
      *
      * @param userID     userID, the user must already exists in the User table
      * @param dictionary the dictionary to be added
      */
      @SqlUpdate("INSERT INTO Dictionary VALUES " +
      "(:dictionary.getDictID, :dictionary.getDictName, :dictionary.toContentString);")
     void insertDictionary(@BindMethods("dictionary") Dictionary dictionary);


     /**
      * Lists all dictionaries of all users stored in database.
      * <p>
      * --Note: Whenever we ask JDBI to convert the result to a Dictionary Object,
      * --      we must register our custom row mapper.
      */
     @SqlQuery("SELECT * FROM Dictionary")
     @RegisterRowMapper(DictionaryMapper.class)
     List<Dictionary> listAllDictionaries();

     /**
      * Gets a Dictionary object by a dictionary ID, returns null if dictionary is not found.
      */
     @SqlQuery("SELECT * FROM Dictionary WHERE id = :dictID")
     @RegisterRowMapper(DictionaryMapper.class)
     Dictionary getDictionaryByID(@Bind("dictID") String dictID);


     /**
      * Deletes a dictionary by a dictID, do nothing if dictID doesn't exist.
      */
     @SqlUpdate("DELETE FROM DictionaryOfUser WHERE dictID = :dictID;")
     void deleteDictionaryByID(String dictID);

 }