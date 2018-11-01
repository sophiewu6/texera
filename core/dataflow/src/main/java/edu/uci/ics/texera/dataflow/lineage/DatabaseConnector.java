package edu.uci.ics.texera.dataflow.lineage;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.LineageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

public class DatabaseConnector {
    private static final String DB_URL="jdbc:mysql://localhost:3306/";
    private static final String PROPERTIES="?characterEncoding=utf8&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
    private static final String MYSQLDB="mysql";
    private static final String USER="root";
    private static final String PASSWORD="root_pwd";
    private static final String LINEAGE_DB="LineageDB";
    private static Connection lineageDbConn=null;
    public static Statement lineageStat;
    public static final String RESULT_CATALOG="ResultCatalog";
    public static final String LINEAGE_CATALOG="LineageCatalog";
    
    public DatabaseConnector() {
        super();
        try {
            createDB();
            connectDB();
            createCatalogTables();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    public static boolean DatabaseExists(String databaseName){
        boolean exits=false;
        try {
            Connection connection=DriverManager.getConnection(DB_URL+MYSQLDB+PROPERTIES, USER, PASSWORD);
            Statement statement=connection.createStatement();
            ResultSet resultSet=connection.getMetaData().getCatalogs();
            while(resultSet.next()) {
                if(resultSet.getString(1).compareTo(LINEAGE_DB)==0) {
                    exits=true;
                    break;
                }
            }
            statement.close();
            connection.close();
        }catch(SQLException se) {
            se.printStackTrace();
        }
        return exits;
    }
    
    public static void createDB(){
        try {
            Connection connection=DriverManager.getConnection(DB_URL+MYSQLDB+PROPERTIES, USER, PASSWORD);
            boolean exits=false;
            ResultSet resultSet=connection.getMetaData().getCatalogs();
            while(resultSet.next()) {
                if(resultSet.getString(1).compareTo(LINEAGE_DB)==0) {
                    exits=true;
                    break;
                }
            }
            if(!exits) {
                Statement statement=connection.createStatement();
                String createLineageDB="CREATE DATABASE "+LINEAGE_DB;
                statement.executeUpdate(createLineageDB);
                statement.close();
            }
            connection.close();
        } catch (SQLException se) {
            // TODO: handle exception
            se.printStackTrace();
        }
    }
    public static void connectDB() throws SQLException {
        lineageDbConn=DriverManager.getConnection(DB_URL+LINEAGE_DB+PROPERTIES, USER, PASSWORD);
        lineageStat=lineageDbConn.createStatement();
    }
    public static boolean TableExists(String tableName) {
        boolean exist=false;
        try {
            DatabaseMetaData dbm=lineageDbConn.getMetaData();
            ResultSet resultSet=dbm.getTables(null, null, tableName, null);
            if(resultSet.next())exist=true;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return exist;
    }
    public static void createCatalogTables() throws SQLException {
        if(!TableExists(RESULT_CATALOG)) {
            String createResultCatalog="CREATE TABLE "+RESULT_CATALOG+"(operatorName VARCHAR(50) not NULL,resultTableName VARCHAR(60))";
            lineageStat.executeUpdate(createResultCatalog);
        }
        if(!TableExists(LINEAGE_CATALOG)) {
            String createLineageCatalog="CREATE TABLE "+LINEAGE_CATALOG+"(operatorName VARCHAR(50) not NULL,lineageTableName VARCHAR(60))";
            lineageStat.executeUpdate(createLineageCatalog);
        }
    }
    public static void createResultTable(String tableName,Schema schema) {
        ArrayList<Attribute> attributes=new ArrayList<>(schema.getAttributes());
        String createTable="CREATE TABLE "+tableName+" (";
        for(int i=0;i<attributes.size();i++) {
            String attrName=attributes.get(i).getName();
            AttributeType attrType=attributes.get(i).getType();
            if(attrType==AttributeType._ID_TYPE||attrType==AttributeType.DATE||attrType==AttributeType.DATETIME||attrType==AttributeType.LIST||attrType==AttributeType.STRING||attrType==AttributeType.TEXT) {
                createTable+=attrName+" TEXT ";
            }else if(attrType==AttributeType.DOUBLE) {
                createTable+=attrName+" DOUBLE ";
            }else if(attrType==AttributeType.INTEGER){
                createTable+=attrName+" INT ";
            }else {
                throw new TexeraException("Unkown IField class: " + attrType);
            }
            if(i!=attributes.size()-1)createTable+=", ";
            else createTable+=")";
        }
        try {
            if(TableExists(tableName)) {//if the table exists, drop it.
                String dropTable="DROP TABLE "+tableName;
                lineageStat.executeUpdate(dropTable);
            }
            lineageStat.executeUpdate(createTable); //create table
            String alterCharaterEncoding="ALTER TABLE "+tableName+" convert to character set utf8mb4 collate utf8mb4_unicode_ci";
            DatabaseConnector.lineageStat.executeUpdate(alterCharaterEncoding);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public static void createLineageTable(String tableName) {
        try {
            if(TableExists(tableName)) {//if the table exists, drop it.
                String dropTable="DROP TABLE "+tableName;
                lineageStat.executeUpdate(dropTable);
            }
            String createLineageTable="CREATE TABLE "+tableName+" (inputID INT, outputID INT)";
            lineageStat.executeUpdate(createLineageTable);
        } catch (SQLException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
    public static void deleteTupleFromResultCatalogTable(String operatorName) {
        try{
            String deleteTupleFromResultCT="DELETE FROM "+DatabaseConnector.RESULT_CATALOG+" WHERE operatorName ='"+operatorName+"'";
            lineageStat.executeUpdate(deleteTupleFromResultCT);
        }catch(SQLException se) {
            se.printStackTrace();
        }
        
    }
    public static void deleteTupleFromLineageCatalogTable(String operatorName) {
        try{
            String deleteTupleFromLineageCT="DELETE FROM "+DatabaseConnector.LINEAGE_CATALOG+" WHERE operatorName ='"+operatorName+"'";
            lineageStat.executeUpdate(deleteTupleFromLineageCT);
        }catch(SQLException se) {
            se.printStackTrace();
        }
    }
    public static void insertTupleIntoResultCatalogTable(String operatorName) {
        try {
            String writeTupeToResultCT="INSERT INTO "+DatabaseConnector.RESULT_CATALOG+" VALUES('"+operatorName+"','"+operatorName+"Result')";
            lineageStat.executeUpdate(writeTupeToResultCT);
        } catch (SQLException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
    public static void insertTupleIntoLineageCatalogTable(String operatorName) {
        try {
            String writeTupeToLineageCT="INSERT INTO "+DatabaseConnector.LINEAGE_CATALOG+" VALUES('"+operatorName+"','"+operatorName+"Lineage')";
            lineageStat.executeUpdate(writeTupeToLineageCT);
        } catch (SQLException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
    public static void insertTupleToResultTable(String tableName,String tupleContent) {
    	String insertTuple="INSERT INTO "+tableName+" (";
        String insertTuple1=" VALUES (";
        String[] schemaFields=tupleContent.split(", fields=");
        String[] schemas=schemaFields[0].split(", Attribute");
        ArrayList<String> fieldName=new ArrayList<>();
        ArrayList<String> fieldTypes=new ArrayList<>();
        ArrayList<String> fieldValues=new ArrayList<>();
        for(int i=0;i<schemas.length;i++) {
            String schemaString=schemas[i];
            fieldName.add(schemaString.substring(schemaString.indexOf("[name=")+"[name=".length(), schemaString.indexOf(", type=")));
        }
        for(int i=0;i<schemas.length;i++) {
            String schemaString=schemas[i];
            fieldTypes.add(schemaString.substring(schemaString.indexOf(", type=")+", type=".length(), schemaString.indexOf("]")));
        }
        
        String[] fields=schemaFields[1].split("\\[value=");
        for(int i=1;i<fields.length;i++) {
            fieldValues.add(fields[i].substring(0, fields[i].indexOf("]")));
        }
        for(int i=0;i<fieldTypes.size();i++) {
            insertTuple+=fieldName.get(i);
            String fieldType=fieldTypes.get(i);
            if(fieldType.compareTo("_id")==0||fieldType.compareTo("date")==0||fieldType.compareTo("datetime")==0||fieldType.compareTo("list")==0||fieldType.compareTo("string")==0||fieldType.compareTo("text")==0){
                insertTuple1+="'"+fieldValues.get(i)+"'";
            }else if(fieldType.compareTo("double")==0||fieldType.compareTo("integer")==0) {
                insertTuple1+=fieldValues.get(i);
            }else {
                throw new TexeraException("Unkown IField class: " + fieldType);
            }
            if(i!=fieldTypes.size()-1) {
                insertTuple+=",";
                insertTuple1+=",";
            }
            else {
                insertTuple+=")";
                insertTuple1+=")";
            }
        }
        insertTuple+=insertTuple1;
        try {
            lineageStat.executeUpdate(insertTuple);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public static void insertTupleIntoLineageTable(String tableName,int inputID,int outputID) {
        try {
            String insertLineageTuple="INSERT INTO "+tableName+" VALUES("+inputID+","+outputID+")";
            lineageStat.executeUpdate(insertLineageTuple);
        } catch (SQLException e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
    public static int getInputTupleLineageID(Tuple tuple) {
    	if(!tuple.getSchema().containsAttribute(SchemaConstants.LINEAGE_TUPLE_ID)) {
    		throw new LineageException("get input tuple lineage id, but the tuple do not have lineageTupleID attribute");
    	}
        IDField inputTupleLineageID=tuple.getField(SchemaConstants.LINEAGE_TUPLE_ID);
        int inputID=Integer.parseInt(inputTupleLineageID.getValue());
        return inputID;
    }
    public static Tuple addTupleLineageIDToOutput(Tuple outputTuple,int lineageID) {
    	Schema schema=outputTuple.getSchema();
    	//if contains lineageTupleID attribute, update the lineageTupleID with lineageID
    	if(schema.containsAttribute(SchemaConstants.LINEAGE_TUPLE_ID)) {
    		List<IField> inputFields=new ArrayList<>();
            inputFields.addAll(outputTuple.getFields());
            inputFields.remove(schema.getIndex(SchemaConstants.LINEAGE_TUPLE_ID).intValue());
            List<IField> outputFields=new ArrayList<>();
            IDField outputTupleLineageID=new IDField(""+lineageID);
            outputFields.add(outputTupleLineageID);
            outputFields.addAll(inputFields);
            Tuple tupleWithLineageID=new Tuple(schema,outputFields.stream().toArray(IField[]::new));
            return tupleWithLineageID;
    	}else {//if not contain, add the lineageTupleID attribute
    		List<IField> outputFields=new ArrayList<>();
    		IDField outputTupleLineageID=new IDField(""+lineageID);
    		outputFields.add(outputTupleLineageID);
    		outputFields.addAll(outputTuple.getFields());
    		schema=addLineageIDAttribute(schema);
    		Tuple tupleWithLineageID=new Tuple(schema,outputFields.stream().toArray(IField[]::new));
            return tupleWithLineageID;
    	}
    }
    //if the schema doesn't contain lineageTupleID attribute, then add lineageTupleID attribute.
    public static Schema addLineageIDAttribute(Schema schema) {
    	if (! schema.containsAttribute(SchemaConstants.LINEAGE_TUPLE_ID)) {
    		schema = new Schema.Builder().add(SchemaConstants.LINEAGE_TUPLE_ID_ATTRIBUTE).add(schema).build(); 
        }
    	return schema;
    }
    
    public static void dropDB() throws SQLException {
        String dropLineageDB="DROP DATABASE "+LINEAGE_DB;
        lineageStat.executeUpdate(dropLineageDB);
    }
    public static void close() throws SQLException {
        lineageStat.close();
        lineageDbConn.close();
    }
}
