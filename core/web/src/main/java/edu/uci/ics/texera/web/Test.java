package edu.uci.ics.texera.web;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.source.file.FileSourceOperator;
import edu.uci.ics.texera.dataflow.source.file.FileSourcePredicate;

public class Test {
    
    public static void main(String[] args) throws Exception {
        String legalDocFolder = "/Users/zuozhiw/Desktop/legal";
        
        IOperator fileSrc = new FileSourcePredicate(legalDocFolder, "文本", null, null, null).newOperator();
        
        
        
        
    }

}
