package Operator.Source.File;

import Operator.Base.OperatorBase;
import Exception.TexeraException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.Seq;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yuranyan
 * This is the operator that is used for reading a file
 */

public class FileSourceOperator extends OperatorBase {

    private final FileSourcePredicate predicate;

    // a list of files, each of which is a valid text file
    private List<Path> pathList;

    public FileSourceOperator(FileSourcePredicate predicate){
        this.predicate = predicate;
        this.pathList = new ArrayList<>();
        setOutput(new ArrayList<>());

        if(!predicate.getFilePath().startsWith("hdfs")){
            Path filePath = Paths.get(predicate.getFilePath());
            if (! Files.exists(filePath)) {
                throw new TexeraException(String.format("file %s doesn't exist", filePath));
            }

            if (Files.isDirectory(filePath)) {
                try {
                    if (this.predicate.isRecursive()) {
                        pathList.addAll(Files.walk(filePath, this.predicate.getMaxDepth()).collect(Collectors.toList()));
                    } else {
                        pathList.addAll(Files.list(filePath).collect(Collectors.toList()));
                    }

                } catch (IOException e) {
                    throw new TexeraException(String.format(
                            "opening directory %s failed: " + e.getMessage(), filePath));
                }
            } else {
                pathList.add(filePath);
            }

            // filter directories, files starting with ".",
            //   and files that don't end with allowedExtensions
            this.pathList = pathList.stream()
                    .filter(path -> ! Files.isDirectory(path))
                    .filter(path -> ! path.getFileName().startsWith("."))
                    .collect(Collectors.toList());

            // check if the path list is empty
            if (pathList.isEmpty()) {
                throw new TexeraException(String.format(
                        "the filePath: %s doesn't contain any files. ", filePath));
            }
        }
        else {
            pathList.add(Paths.get(predicate.getFilePath()));
        }



    }

    /**
     * This function accept a path as input and return the parallelized RDD reference
     * @param path The path of the input file
     * @return The parallelized input file
     */
    Dataset read(String path){
        Dataset file;
        file =  sparkSession.read().textFile(path);
        return file;
    }

    /**
     * This function does same operation as the last one, except it accept another input as min partition number.
     * This function only process text file
     * @param path The path of the input file
     * @param minPartition The min number of partition for the input text file
     * @return The parallelized input file
     */
    Dataset<String> readPartitionText(String path, int minPartition){
        Dataset<String> file = sparkSession.read().textFile(path);
//        file =  sparkSession.read().option("wholetext",true).textFile(path);
        return file.coalesce(minPartition);
    }

    /**
     * This function does same operation as the last one, except it read files with format other than txt
     * @param path
     * @param minPartition
     * @return
     */
    Dataset<Row> readPatition(Path path, int minPartition){
        String p = path.toString();
        if(p.startsWith("hdfs")){
            p = p.substring(0,5) + '/' + p.substring(5);
        }
        Dataset<Row> file = sparkSession.read().format("csv").load(p);
        return  file;
    }

    @Override
    public void execute() throws TexeraException {


        for(Path p:this.pathList){
            try {
                //Read file based on its extension
//                String extension = com.google.common.io.Files.getFileExtension(p.toString());
//                String content;
//                if (extension.equalsIgnoreCase("pdf")) {
//                    content = FileExtractorUtils.extractPDFFile(path);
//                } else if (extension.equalsIgnoreCase("ppt") || extension.equalsIgnoreCase("pptx")) {
//                    content = FileExtractorUtils.extractPPTFile(path);
//                } else if(extension.equalsIgnoreCase("doc") || extension.equalsIgnoreCase("docx")) {
//                    content = FileExtractorUtils.extractWordFile(path);
//                } else {
//                    content = FileExtractorUtils.extractPlainTextFile(path);

                //Change the dataset to dataFrame, and rename the column
                Dataset dataset;
                Dataset<Row> dataFrame;

                if(p.endsWith("1.txt")) {
                    dataset = readPartitionText(p.toString(), 72);
                    List<String> name = new ArrayList<>();
                    name.add(0, predicate.getAttributeName());
                    Seq newName = scala.collection.JavaConverters.collectionAsScalaIterableConverter(name).asScala().toSeq();
                    dataFrame = dataset.toDF(newName);
                    System.out.println(1);
                }
                else {
                    dataFrame = readPatition(p, 72);
                    System.out.println("Partition number");
                    System.out.println(dataFrame.rdd().getNumPartitions());
                }




                if(getOutput().size() == 0){
                    addOutput(dataFrame);
                }
                else {
                    changeOutput(dataFrame.union(getOutput().get(0)),0);
                }


            } catch (TexeraException e) {
                // ignore error and move on
                // TODO: use log4j
                System.out.println("FileSourceOperator: file read error, file is ignored. " + e.getMessage());
            }
            super.execute();
        }

    }
}
