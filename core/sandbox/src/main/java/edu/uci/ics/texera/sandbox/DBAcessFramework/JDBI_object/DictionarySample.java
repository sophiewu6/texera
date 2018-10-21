package edu.uci.ics.texera.sandbox.DBAcessFramework.JDBI_object;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.Arrays;
import java.util.List;

/**
 * This is a sample to use JDBI API to access database.
 *
 * @author Zuozhi Wang
 * Created at 10/21/2018
 */
public class DictionarySample {

    public static void main(String[] args) throws Exception {
        Jdbi jdbi = getJdbi();

        // open a new connection to h2 in memory database attached to handle
        Handle handle = jdbi.open();

        // generate actual DAO object from JDBI
        UserDAO userDAO = handle.attach(UserDAO.class);
        DictionaryDAO dictionaryDAO = handle.attach(DictionaryDAO.class);

        // set up tables
        userDAO.createTable();
        dictionaryDAO.createTable();

        // test inserting some data
        userDAO.insertUser(new User("1", "anteater"));
        userDAO.insertUser(new User("2", "anteater"));

        dictionaryDAO.insertDictionary("1",
                new Dictionary("dict1_1", "dict 1 user 1", Arrays.asList("hello", "UCI")));
        dictionaryDAO.insertDictionary("2",
                new Dictionary("dict2_1", "dict 1 user 2", Arrays.asList("hi", "texera")));
        dictionaryDAO.insertDictionary("2",
                new Dictionary("dict2_2", "dict 2 user 2", Arrays.asList("sample", "test")));

        // query some data
        List<User> allUsers = userDAO.listUsers();
        System.out.println("all users");
        System.out.println(allUsers);

        List<Dictionary> user1Dicts = dictionaryDAO.listDictionariesOfUser("1");
        System.out.println("user 1 dicts");
        System.out.println(user1Dicts);

        List<Dictionary> user2Dicts = dictionaryDAO.listDictionariesOfUser("2");
        System.out.println("user 2 dicts");
        System.out.println(user2Dicts);

        dictionaryDAO.deleteDictionaryByID("dict2_2");
        user2Dicts = dictionaryDAO.listDictionariesOfUser("2");
        System.out.println("user 2 dict after delete");
        System.out.println(user2Dicts);

    }

    public static Jdbi getJdbi() {
        // use h2 in memory database
        return Jdbi.create("jdbc:h2:mem:dictsample")
                .installPlugin(new SqlObjectPlugin());
    }


}
