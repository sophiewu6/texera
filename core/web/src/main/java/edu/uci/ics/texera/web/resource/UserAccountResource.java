package edu.uci.ics.texera.web.resource;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.impl.DSL;

import edu.uci.ics.texera.web.TexeraWebException;
import edu.uci.ics.texera.web.resource.generated.tables.records.UseraccountRecord;
import edu.uci.ics.texera.web.response.GenericWebResponse;

import static edu.uci.ics.texera.web.resource.generated.Tables.*;
import static org.jooq.impl.DSL.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;


@Path("/users/accounts/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class UserAccountResource {
    private final static String serverName = "root";
    private final static String password = "PassWithWord";
    private final static String url = "jdbc:mysql://localhost:3306/texera";
    
    
    /**
     * Corresponds to `src/app/dashboard/type/user-account.ts`
     */
    public static class UserAccount {
        public String userName;
        public double userID;
        
        public static UserAccount generateErrorAccount() {
            return new UserAccount("", -1);
        }

        public UserAccount(String userName, double userID) {
            this.userName = userName;
            this.userID = userID;
        }
    }
    
    /**
     * Corresponds to `src/app/dashboard/type/user-account.ts`
     */
    public static class UserAccountResponse {
        public int code; // 0 represents success and 1 represents error
        public UserAccount userAccount;
        public String message;
        
        public static UserAccountResponse generateErrorResponse(String message) {
            return new UserAccountResponse(1, UserAccount.generateErrorAccount(), message);
        }
        
        public static UserAccountResponse generateSuccessResponse(UserAccount userAccount) {
            return new UserAccountResponse(0, userAccount, "");
        }

        public UserAccountResponse(int code, UserAccount userAccount, String message) {
            this.code = code;
            this.userAccount = userAccount;
            this.message = message;
        }
    }
    
    @GET
    @Path("/login")
    public UserAccountResponse login(String userName) {

        Condition loginCondition = USERACCOUNT.USERNAME.equal(userName); // TODO compare password
        Result<Record1<Integer>> result = getUserID(loginCondition);
    	
    	if (result.size() == 0) { // not found
    	    return UserAccountResponse.generateErrorResponse("The username or password is incorrect");
    	} else {
    	    UserAccount account = new UserAccount(
    				result.get(0).get(USERACCOUNT.USERNAME),
    				result.get(0).get(USERACCOUNT.USERID));
    	    UserAccountResponse response = UserAccountResponse.generateSuccessResponse(account);
    		return response;
    	}
    	
    }
    
    @PUT
    @Path("/register")
    public UserAccountResponse register(String userName) {
    
        Condition registerCondition = USERACCOUNT.USERNAME.equal(userName);
        Result<Record1<Integer>> result = getUserID(registerCondition);
        
        if (result.size() == 0) { // not found and register is allowed
            return insertUserAccount(userName);
        } else {
            return UserAccountResponse.generateErrorResponse("Username already exists");
        }
    }
    
    private Result<Record1<Integer>> getUserID(Condition condition) {
        try (Connection conn = DriverManager.getConnection(url, serverName, password)) {
            DSLContext create = DSL.using(conn, SQLDialect.MYSQL);
            Result<Record1<Integer>> result = create
                    .select(USERACCOUNT.USERID)
                    .from(USERACCOUNT)
                    .where(condition)
                    .limit(1)
                    .fetch();
            return result;
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    }
    
    private UserAccountResponse insertUserAccount(String userName) {
        try (Connection conn = DriverManager.getConnection(url, serverName, password)) {
            DSLContext create = DSL.using(conn, SQLDialect.MYSQL);
            
            create.insertInto(USERACCOUNT,
                    USERACCOUNT.USERNAME)
                    .values(userName)
                    .execute();
            
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
        return login(userName);
    }
    
}
