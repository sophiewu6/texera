package NaivePerformanceTest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkDataFrame {

    public static void main(String[] arg){

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("DataFrame Test")
                .config("spark.master", "local[6]")
                .getOrCreate();
        String path = "/home/yuran/Desktop/Six_g_input.csv";
        Dataset<Row> DSTest = sparkSession.read().csv(path);

        long count1 = DSTest.filter(col("_c0").contains("Asia")).count();
        System.out.println(count1);
    }

}
