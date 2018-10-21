package edu.uci.ics.texera.sandbox.DBAcessFramework.JDBI_object;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;

/**
 * UserDAO communicates with the database through JDBI for User related tasks.
 * UserDAO provides:
 *  - create tables
 *  - query a specific user
 *  - insert a new user
 *
 * -----------
 * This is a sample usage of JDBI's SQL Declarative API.
 * http://jdbi.org/#_declarative_api
 * <p>
 * DAO class (Data Access Object) is a common design pattern that encapsulates
 * accessing database and making SQL queries as function calls.
 * <p>
 * JDBI could use the SQL code in the annotation to *automatically* generate the implementation,
 * without having to write low-level JDBC code over and over again.
 *
 * @author Zuozhi Wang
 * Created at 10/21/2018
 *
 */
public interface UserDAO {

    /**
     * Creates the User table with
     * id as primary key, and user name.
     */
    @SqlUpdate("" +
            "CREATE TABLE USER " +
            "(id VARCHAR PRIMARY KEY, username VARCHAR);"
    )
    void createTable();

    /**
     * Adds a new User into the database.
     * The User ID must be unique (such as a random UUID).
     *
     * --Note: @BindBean means JDBI automatically analyze the java bean class and bind its field names
     */
    @SqlUpdate("INSERT INTO User VALUES " +
            "(:user.id, :user.username);")
    void insertUser(
            @BindBean("user") User user);

    /**
     * Lists all users.
     */
    @SqlQuery("SELECT * from User")
    @RegisterBeanMapper(User.class)
    List<User> listUsers();


    /**
     * Finds a user by the user ID, returns null if it doesn't exist
     */
    @SqlQuery("SELECT * FROM User where id = :userID")
    @RegisterBeanMapper(User.class)
    User getUser(@Bind("userID") String userID);




}
