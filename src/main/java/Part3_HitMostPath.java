
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Part3_HitMostPath {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("mycc2_HitMostPath").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> distFile = sc.textFile("output/");
        System.out.println(distFile.first());
        //JavaRDD<String> counts = distFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator());;
        //JavaRDD<String> result1 = distFile.filter(s -> s.contains("(http\\:\\/\\/www\\.the\\-associates\\.co\\.uk)?([^\\?\\&\\,]+)"));






    }


}

