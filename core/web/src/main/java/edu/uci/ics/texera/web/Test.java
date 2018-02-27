package edu.uci.ics.texera.web;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.source.file.FileSourceOperator;
import edu.uci.ics.texera.dataflow.source.file.FileSourcePredicate;

public class Test {
    
    public static void main(String[] args) throws Exception {
        String legalDocFolder = "/Users/zuozhiw/Desktop/legal";
        
        List<Path> files = Files.list(Paths.get(legalDocFolder)).collect(Collectors.toList());
        
        List<Path> wrongFiles = new ArrayList<>();
        
        int counter = 0;
        for (Path path: files) {
            System.out.println(counter);
            counter++;
            
            FileSourceOperator fileSrc = new FileSourcePredicate(path.toRealPath().toString(), "text", null, null).newOperator();
            fileSrc.open();
            Tuple tuple = fileSrc.getNextTuple();
            if (tuple == null) {
                wrongFiles.add(path);
                System.out.println(path.getFileName());
                continue;
            }
            
            TextField field = tuple.getField("text", TextField.class);
            String text = field.getValue();
            
            if (! text.contains("无效宣告请求审查决定书")) {
                System.out.println(path.getFileName());
                wrongFiles.add(path);
            }
            
            
            fileSrc.close();
            
        }
        
        for (Path path: wrongFiles) {
            System.out.println(path.getFileName());
        }
        
        
    }

}
