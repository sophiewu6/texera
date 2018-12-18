package edu.uci.ics.texera.perftest.bbcnews;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.perftest.utils.PerfTestUtils;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class BBCnewsSample {
    public static String bbcnewsFilePath = PerfTestUtils.getResourcePath("/testData").toString();
    public static String bbcnewsTable = "bbcnews_sample";
    
    public static void main(String[] args) throws Exception {
        writeBBCnewsIndex();
    }
    
    public static void writeBBCnewsIndex() throws Exception {
        RelationManager relationManager = RelationManager.getInstance();
        relationManager.deleteTable(bbcnewsTable);
        relationManager.createTable(bbcnewsTable, Utils.getDefaultIndexDirectory().resolve(bbcnewsTable), BBCnewsSchema.BBCNEWS_SCHEMA, 
                LuceneAnalyzerConstants.standardAnalyzerString());
        
        DataWriter dataWriter = relationManager.getTableDataWriter(bbcnewsTable);
        dataWriter.open();
        
        File bbcnewsFile=new File(bbcnewsFilePath);
        String[] bbcnewsFileNames=bbcnewsFile.list();
        for(int i=0;i<bbcnewsFileNames.length;i++) {
    		InputStreamReader inStrR = new InputStreamReader(new FileInputStream(bbcnewsFilePath+"/"+bbcnewsFileNames[i]));
    		BufferedReader br = new BufferedReader(inStrR);
    		String text="";
    		String line=br.readLine();
    		while (line!=null) {
				text+=line+"\n";
				line=br.readLine();
			}
    		br.close();
    		Tuple tuple=new Tuple(BBCnewsSchema.BBCNEWS_SCHEMA,new TextField(text));
    		dataWriter.insertTuple(tuple);
        }
        dataWriter.close();
    }
}
