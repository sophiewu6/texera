package edu.uci.ics.texera.perftest.twitterwithhivlabel;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

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
        
//        File bbcnewsFile=new File(twitterHivFilePath);
		InputStreamReader inStrR = new InputStreamReader(new FileInputStream(twitterHivFilePath+"/"+"hiv_test.pos"));
		BufferedReader br = new BufferedReader(inStrR);
		String line=br.readLine();
		while (line!=null) {
			Tuple tuple=new Tuple(TwitterWithHivLabeSchema.TWITTER_HIV_LABEL_SCHEMA,new TextField(line),new StringField("POS"));
			dataWriter.insertTuple(tuple);
			line=br.readLine();
			line=br.readLine();
		}
		br.close();
		inStrR=new InputStreamReader(new FileInputStream(twitterHivFilePath+"/"+"hiv_test.neg"));
		br=new BufferedReader(inStrR);
		line=br.readLine();
		while (line!=null) {
			Tuple tuple=new Tuple(TwitterWithHivLabeSchema.TWITTER_HIV_LABEL_SCHEMA,new TextField(line),new StringField("NEG"));
			dataWriter.insertTuple(tuple);
			line=br.readLine();
			line=br.readLine();
		}
		br.close();
		dataWriter.close();
    }
}
