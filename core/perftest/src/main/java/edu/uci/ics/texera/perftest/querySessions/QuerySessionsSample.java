package edu.uci.ics.texera.perftest.querySessions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.perftest.twitterwithhivlabel.TwitterWithHivLabeSchema;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class QuerySessionsSample {
    public static String querySessionFilePath = PerfTestUtils.getResourcePath("/querySessions").toString();
    public static String querySessionTable = "querySessionTable";
    
    public static void main(String[] args) throws Exception {
    	writeQuerySessionIndex();
    }
    public static void writeQuerySessionIndex() throws Exception {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(querySessionTable);
        relationManager.createTable(querySessionTable, Utils.getDefaultIndexDirectory().resolve(querySessionTable),QuerySessionsSchema.QUERY_SESSIONS_SCHEMA, 
                LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter dataWriter = relationManager.getTableDataWriter(querySessionTable);
        dataWriter.open();
        
        InputStreamReader inStrR = new InputStreamReader(new FileInputStream(querySessionFilePath+"/session.test"));
        BufferedReader br = new BufferedReader(inStrR);
        String line=br.readLine();
        while(line!=null) {
        	Tuple tuple=new Tuple(QuerySessionsSchema.QUERY_SESSIONS_SCHEMA,new TextField(line));
			dataWriter.insertTuple(tuple);
			line=br.readLine();
        }
        br.close();
        dataWriter.close();
    }
}
