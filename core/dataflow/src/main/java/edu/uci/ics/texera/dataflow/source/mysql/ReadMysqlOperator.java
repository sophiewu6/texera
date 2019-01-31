package edu.uci.ics.texera.dataflow.source.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

public class ReadMysqlOperator implements ISourceOperator {
    private final ReadMysqlPredicate predicate;
    private int cursor = CLOSED;
    private Connection connection;
    private Statement statement;
    private PreparedStatement prepStatement;
    private ResultSet result;
    private ResultSetMetaData rsmd;
    private List<Integer> columnTypes;

    public ReadMysqlOperator(ReadMysqlPredicate predicate) {
        this.predicate = predicate;
    }
    
    public void getResult() throws TexeraException {
    	// get query result and store its information
    	String sqlStatemnt = "SELECT * FROM " + predicate.getTable();
        try {
            prepStatement = connection.prepareStatement(sqlStatemnt);
            result = prepStatement.executeQuery();
            rsmd = result.getMetaData();
            updateColumnTypes();
        } catch (SQLException e) {
            throw new DataflowException(
                    "ReadMysql processTuples fails to execute prepared statement. " + e.getMessage());
        }
    }

    @Override
    public void open() throws TexeraException {
        if (cursor == OPENED) {
            return;
        }
        // JDBC connection
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String url = "jdbc:mysql://" + predicate.getHost() + ":" + predicate.getPort() + "/"
                    + predicate.getDatabase() + "?autoReconnect=true&useSSL=true";
            this.connection = DriverManager.getConnection(url, predicate.getUsername(), predicate.getPassword());
            statement = connection.createStatement();
            cursor = OPENED;
            getResult();
        } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new DataflowException("ReadMysql failed to connect to mysql database." + e.getMessage());
        }
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        try {
        	if (result.next()) {
            	Tuple.Builder builder = new Tuple.Builder();
            	int colNum = rsmd.getColumnCount();
            	for (int i = 0; i < colNum; ++i) {
            		switch (columnTypes.get(i)) {
            		case 1:
            			builder.add(rsmd.getColumnName(i), AttributeType.INTEGER, new IntegerField(result.getInt(i)));
            			break;
            		case 2:
            			builder.add(rsmd.getColumnName(i), AttributeType.DOUBLE, new DoubleField(result.getDouble(i)));
            			break;
            		case 3:
            			builder.add(rsmd.getColumnName(i), AttributeType.DATE, new DateField(result.getDate(i)));
            			break;
            		case 4:
            			builder.add(rsmd.getColumnName(i), AttributeType.STRING, new StringField(result.getString(i)));
            			break;
            		}
            	}
            	return builder.build();
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataflowException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        try {
            if (statement != null)
                statement.close();
            if (prepStatement != null)
                prepStatement.close();
            connection.close();
            cursor = CLOSED;
        } catch (SQLException e) {
            throw new DataflowException("ReadMysql fail to close. " + e.getMessage());
        }
    }
    
    @Override
    public Schema getOutputSchema() throws TexeraException {
    	return null;
    }
    
    @Override
	public Schema transformToOutputSchema(Schema... inputSchema) {
		// TODO Auto-generated method stub
		return null;
	}
    
    // Helper Function
    private void updateColumnTypes() {
    	// Store types of each column
        /*
            -7	BIT
			-6	TINYINT
			-5	BIGINT
			-4	LONGVARBINARY 
			-3	VARBINARY
			-2	BINARY
			-1	LONGVARCHAR
			0	NULL
			1	CHAR
			2	NUMERIC
			3	DECIMAL
			4	INTEGER
			5	SMALLINT
			6	FLOAT
			7	REAL
			8	DOUBLE
			12	VARCHAR
			91	DATE
			92	TIME
			93	TIMESTAMP
			1111 	OTHER
        */
    	try {
    		columnTypes = new ArrayList<>();
            int colNum = rsmd.getColumnCount();
            for (int i = 0; i < colNum; ++i) {
            	switch (rsmd.getColumnType(i)) {
            	case 4:
            		// Integer
            		columnTypes.add(1);
            		break;
            	case 6:
            	case 8:
            		// Double
            		columnTypes.add(2);
            		break;
            	case 91:
            		// Date
            		columnTypes.add(3);
            		break;
            	default:
            		// String
            		columnTypes.add(4);
            		break;
            	}
            }
    	} catch (SQLException e) {
            throw new DataflowException("ReadMysql fail to close. " + e.getMessage());
        }
    }
}
