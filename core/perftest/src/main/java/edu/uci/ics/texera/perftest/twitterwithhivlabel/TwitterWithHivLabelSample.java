package edu.uci.ics.texera.perftest.twitterwithhivlabel;

import java.io.FileReader;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class TwitterWithHivLabelSample {
    public static String twitterHivFilePath = PerfTestUtils.getResourcePath("/twitterHivLabel").toString();
    public static String twitterHivLabelTable = "twitter_hiv_label";
    
    public static void main(String[] args) throws Exception {
        writeTwitterHivIndex();
    }
    public static void writeTwitterHivIndex() throws Exception {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(twitterHivLabelTable);
        relationManager.createTable(twitterHivLabelTable, Utils.getDefaultIndexDirectory().resolve(twitterHivLabelTable), TwitterWithHivLabeSchema.TWITTER_HIV_LABEL_SCHEMA, 
                LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter dataWriter = relationManager.getTableDataWriter(twitterHivLabelTable);
        dataWriter.open();
        
        CSVReader csvReader = new CSVReader(new FileReader(twitterHivFilePath+"/"+"hiv_test.csv"));
        List<String[]> allRows = csvReader.readAll();
        csvReader.close();
        for(int i=1;i<allRows.size();i++) {
        	String[] row=allRows.get(i);
			Tuple tuple=new Tuple(TwitterWithHivLabeSchema.TWITTER_HIV_LABEL_SCHEMA,new TextField(row[0]),new StringField(row[1]));
			dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
    }
}
