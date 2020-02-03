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

        public UserAccount(String userName, double userID) {
            this.userName = userName;
            this.userID = userID;
        }
        public UserAccount() {
            this.userName = "";
            this.userID = -1;
        }
    }
    
    /**
     * Corresponds to `src/app/dashboard/type/user-account.ts`
     */
    public static class UserAccountResponse {
        public int code; // 0 represents success and 1 represents error
        public UserAccount userAccount;

        public UserAccountResponse(int code, UserAccount userAccount) {
            this.code = code;
            this.userAccount = userAccount;
        }
    }
    
    
    
    @GET
    @Path("/login")
    public UserAccountResponse login(String userName) {
        Condition loginCondition = USERACCOUNT.USERNAME.equal(userName);
        Result<Record> result;

        try (Connection conn = DriverManager.getConnection(url, serverName, password)) {
            DSLContext create = DSL.using(conn, SQLDialect.MYSQL);
            result = create.select()
                    .from(USERACCOUNT)
                    .where(loginCondition)
                    .limit(1)
                    .fetch();
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
    	
    	UserAccountResponse response;
    	UserAccount account;
    	if (result.size() == 0) { // not found
    	    return generateErrorResponse();
    	} else {
    		account = new UserAccount(
    				result.get(0).get(USERACCOUNT.USERNAME),
    				result.get(0).get(USERACCOUNT.USERID));
    		response = new UserAccountResponse(0, account);
    		return response;
    	}
    	
    }
    
    
    
    @PUT
    @Path("/register")
    public UserAccountResponse register(String userName) {
        Condition registerCondition = USERACCOUNT.USERNAME.equal(userName);
        Result<Record> result;
        DSLContext create;
    
        try (Connection conn = DriverManager.getConnection(url, serverName, password)) {
            create = DSL.using(conn, SQLDialect.MYSQL);
            result = create.select()
                    .from(USERACCOUNT)
                    .where(registerCondition)
                    .limit(1)
                    .fetch();
        } catch (Exception e) {
            throw new TexeraWebException(e);
        }
        
        if (result.size() == 0) { // not found and register is allowed
            try (Connection conn = DriverManager.getConnection(url, serverName, password)) {
                create = DSL.using(conn, SQLDialect.MYSQL);
                
                create.insertInto(USERACCOUNT,
                        USERACCOUNT.USERNAME)
                        .values(userName)
                        .execute();
                
            } catch (Exception e) {
                throw new TexeraWebException(e);
            }

            return this.login(userName);
        } else {
            return generateErrorResponse();
        }
    }
    
    private UserAccountResponse generateErrorResponse() {
        return new UserAccountResponse(1, new UserAccount());
    }
}
