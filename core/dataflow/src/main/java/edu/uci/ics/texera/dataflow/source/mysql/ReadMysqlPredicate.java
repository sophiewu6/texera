package edu.uci.ics.texera.dataflow.source.mysql;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class ReadMysqlPredicate extends PredicateBase {
    
    private final String host;
    private final Integer port;
    private final String database;
    private final String table;
    private final String query;
    private final String constraint;
    private final String username;
    private final String password;
    
    @JsonCreator
    public ReadMysqlPredicate(
            @JsonProperty(value = PropertyNameConstants.MYSQL_HOST, required=true, defaultValue = "localhost")
            String host,
            @JsonProperty(value = PropertyNameConstants.MYSQL_PORT, required=true, defaultValue = "3306")
            Integer port,
            @JsonProperty(value = PropertyNameConstants.MYSQL_DATABASE, required=true)
            String database,
            @JsonProperty(value = PropertyNameConstants.MYSQL_TABLE, required=true)
            String table,
            @JsonProperty(value = PropertyNameConstants.MYSQL_QUERY, required=false)
            String query,
            @JsonProperty(value = PropertyNameConstants.MYSQL_CONSTRAINT, required=false)
            String constraint,
            @JsonProperty(value = PropertyNameConstants.MYSQL_USERNAME, required=true)
            String username,
            @JsonProperty(value = PropertyNameConstants.MYSQL_PASSWORD, required=true)
            String password
            ) {

    	if (host == null || host.isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_NAME_EXCEPTION);
        }
    	if (database == null || database.isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_NAME_EXCEPTION);
        }
        if (table == null || table.isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_NAME_EXCEPTION);
        }
        if (query == null || query.trim().isEmpty()) {
        	query = "SELECT *";
        }
        if (constraint == null || constraint.trim().isEmpty()) {
        	constraint = "";
        }
        if (username == null || username.isEmpty()) {
            throw new TexeraException(PropertyNameConstants.EMPTY_NAME_EXCEPTION);
        }
        this.host = host.trim();
        this.port = port;
        this.database = database.trim();
        this.table = table.trim();
        this.query = query;
        this.constraint = constraint;
        this.username = username.trim();
        this.password = password;
    }

    @JsonProperty(value = PropertyNameConstants.MYSQL_HOST)
    public String getHost() {
        return host;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_PORT)
    public Integer getPort() {
        return port;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_DATABASE)
    public String getDatabase() {
        return database;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_TABLE)
    public String getTable() {
        return table;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_QUERY)
    public String getQuery() {
        return query;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_CONSTRAINT)
    public String getConstraint() {
        return constraint;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_USERNAME)
    public String getUsername() {
        return username;
    }
    
    @JsonProperty(value = PropertyNameConstants.MYSQL_PASSWORD)
    public String getPassword() {
        return password;
    }
    
    @JsonCreator
    public ReadMysqlOperator newOperator() {
        return new ReadMysqlOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Source: Read Mysql")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Read database and display all columns")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.SOURCE_GROUP)
            .build();
    }
    
}
