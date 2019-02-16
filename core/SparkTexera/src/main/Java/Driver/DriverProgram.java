package Driver;

import Operator.Base.OperatorBase;
import LogicalPlan.LogicalPlan;

import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * The main entrance of the program
 * @author yuranyan
 */

public class DriverProgram {

    /**
     * The entrance of the Driver.DriverProgram program
     * @param arg the argument that will be passed to the main function
     */
//    public static void main(String[] arg){
//
////        Scanner reader = new Scanner(System.in);
////        SparkConf conf = new SparkConf().setAppName("SimpleTexera").setMaster("spark://ip-172-31-19-58.us-east-2.compute.internal:7077");
////        JavaSparkContext sc = new JavaSparkContext(conf);
////
////        System.out.println();
////        System.out.println();
////        System.out.println("Enter the name of the input file, enter Q for exit:");
////        String path = reader.next();
////
////        while(!path.equals("Q")){
////            JavaRDD input = new InputOperator(sc).parallelizeFile(path,4);
////            JavaRDD result = new KeyWordMatcher().processRDD(input, "World");
////            List output = new SinkOperator().sink(result);
////            for(int i = 0; i < output.size(); i++){
////                System.out.println(output.get(i));
////            }
////
////
////
////            System.out.println();
////            System.out.println();
////            System.out.println("Enter the name of the input file, enter Q for exit:");
////            path = reader.next();
////        }
////
////        reader.close();
//
//
//        Scanner reader = new Scanner(System.in);
//        SparkSession sparkSession = SparkSession
//                .builder()
//                .appName("Java Spark SQL basic example")
//                .config("spark.master", "local[*]")
//                .getOrCreate();
//
//        LogicalPlan plan = new LogicalPlan();
//
//        HashMap<String, OperatorBase> DAG = plan.buildQueryPlan(sparkSession);
//        List<String> order = plan.getTopological();
//
//        for(String operatorID: order){
//            DAG.get(operatorID).execute();
//        }
//
//        List<String> sinkList = plan.getSinkList();
//
//    }


}
