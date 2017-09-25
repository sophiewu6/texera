package edu.uci.ics.texera.perftest.wikipedia;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.utils.StorageUtils;

/*
 * This class defines Medline data schema.
 * It also provides functions to read Medline file 
 * from local machine, parse json format,
 * and return a generated ITuple one by one.
 * 
 * @author Zuozhi Wang
 * @author Jinggang Diao
 */
public class WikipediaIndexWriter {

    public static final String ID = "id";
    public static final String TITLE = "title";
    public static final String TEXT = "text";

    public static final Attribute ID_ATTR = new Attribute(ID, AttributeType.STRING);
    public static final Attribute TITLE_ATTR = new Attribute(TITLE, AttributeType.STRING);
    public static final Attribute TEXT_ATTR = new Attribute(TEXT, AttributeType.TEXT);

    public static final Attribute[] ATTRIBUTES_WIKIPEDIA = { ID_ATTR, TITLE_ATTR, TEXT_ATTR};

    public static final Schema SCHEMA_WIKIPEDIA = new Schema(ATTRIBUTES_WIKIPEDIA);

    public static Tuple recordToTuple(String record) throws IOException, ParseException {
        try {
            JsonNode jsonNode = new ObjectMapper().readValue(record, JsonNode.class);
            ArrayList<IField> fieldList = new ArrayList<IField>();
            for (Attribute attr : ATTRIBUTES_WIKIPEDIA) {
            	fieldList.add(StorageUtils.getField(attr.getType(), jsonNode.get(attr.getName()).toString()));
            }
            IField[] fieldArray = new IField[fieldList.size()];
            Tuple tuple = new Tuple(SCHEMA_WIKIPEDIA, fieldList.toArray(fieldArray));
            return tuple;
        } catch (Exception e) {
            return null;
        }
    }
    
    public static void writeWikipediaIndex(Path wikipediaFilepath, String tableName) throws IOException, StorageException, ParseException {
        RelationManager relationManager = RelationManager.getInstance();
        DataWriter dataWriter = relationManager.getTableDataWriter(tableName);
        dataWriter.open();
        
        BufferedReader reader = Files.newBufferedReader(wikipediaFilepath);
        String line;
        while ((line = reader.readLine()) != null) {
            try {
                dataWriter.insertTuple(recordToTuple(line));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        reader.close();
        dataWriter.close(); 
    }

}
