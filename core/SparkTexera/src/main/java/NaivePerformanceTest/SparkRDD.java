package NaivePerformanceTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Date;

public class SparkRDD {

    public static void main(String[] arg){

        SparkConf conf = new SparkConf().setAppName("RDD Test").setMaster("local[6]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "/home/yuran/Desktop/Six_g_input.csv";
        JavaRDD<Record> rddTest = sc.textFile(path).map(
                new Function<String, Record>() {
                    @Override
                    public Record call(String line) throws Exception {
                        String[] fields = line.split(",");
                        Record r = new Record(fields[0],fields[1],fields[2],fields[3],fields[4],fields[5],Long.parseLong(fields[6]),
                                fields[7],Integer.parseInt(fields[8]), Float.parseFloat(fields[9]),Float.parseFloat(fields[10]),
                                Float.parseFloat(fields[11]),Float.parseFloat(fields[12]),Float.parseFloat(fields[13]));
                        return r;
                    }
                }
        );

        long count1 = rddTest.filter(x -> x.continent.contains("Asia")).count();

        System.out.println(count1);
    }
}
