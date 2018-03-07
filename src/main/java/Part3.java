import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Part3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("mycc2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        /*JavaRDD<String> distFile = sc.textFile("src/main/resources/aaa").map(line -> {

            String[] parts = line.split(" ");
                    return parts[6];
                }
        );*/
        JavaRDD<String> distFile = sc.textFile("src/main/resources/access_log");

      /*  while (distFile..hasMoreTokens()) {
            Matcher m = p.matcher(itr.nextToken());
            while (m.find()) {
                word.set(m.group());
                context.write(word, one);
            }
        }*/
        //JavaRDD<String> counts = distFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaRDD<String> result = distFile.filter(s -> s.contains("/assets/img/loading.gif"));
        JavaRDD<String> result1 = distFile.filter(s -> s.contains("/assets/js/lightbox.js"));
        System.out.println("/assets/img/loading.gif: "+result.count());
        System.out.println("/assets/js/lightbox.js: "+result1.count());
       /* JavaPairRDD<String, Integer> finalresult=result.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        System.out.println(finalresult.collect());*/

        //  .mapToPair(word -> new Tuple2<>(word, 1))
        //  .reduceByKey((a, b) -> a + b);
        //System.out.println(distFile.collect());
    }
}
