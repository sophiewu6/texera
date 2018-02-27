package edu.uci.ics.texera.web;

import java.nio.file.Path;

import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import edu.uci.ics.texera.dataflow.source.file.FileSourceOperator;
import edu.uci.ics.texera.dataflow.source.file.FileSourcePredicate;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;
import edu.uci.ics.texera.storage.constants.LuceneAnalyzerConstants;

public class IngestLegal {

    public static void ingestLegalData() throws Exception {
        
        RelationManager relationManager = RelationManager.getInstance();
        
        relationManager.deleteTable("专利判决文档");
        relationManager.createTable("专利判决文档", Utils.getDefaultIndexDirectory().resolve("legal"), 
                new Schema.Builder().add("文本", AttributeType.TEXT).build(), LuceneAnalyzerConstants.chineseAnalyzerString());
        
        relationManager.deleteTable("专利判决文档-100份");
        relationManager.createTable("专利判决文档-100份", Utils.getDefaultIndexDirectory().resolve("legal-100-docs"), 
                new Schema.Builder().add("文本", AttributeType.TEXT).build(), LuceneAnalyzerConstants.chineseAnalyzerString());
        
        Path legalPath = Utils.getResourcePath("legal-1", TexeraProject.TEXERA_WEB);

        FileSourceOperator fileSource = new FileSourcePredicate(legalPath.toRealPath().toString(), "文本", null, null).newOperator();
        
        fileSource.open();
        
        Tuple tuple;
        
        int counter = 0;
        DataWriter writer = relationManager.getTableDataWriter("专利判决文档");
        DataWriter writer100 = relationManager.getTableDataWriter("专利判决文档-100份");
        writer.open();
        writer100.open();

        while ((tuple = fileSource.getNextTuple()) != null) {
            if (counter < 100) {
                writer100.insertTuple(tuple);
            }
            writer.insertTuple(tuple);
            counter++;
            System.out.println("processed: " + counter);
        }
        
        writer.close();
        writer100.close();
        
        fileSource.close();
        
        
    }
    
}
