import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes a roll up-style query against Apache logs.
 * <p>
 * Usage: JavaLogQuery [logFile]
 */
public final class JavaLogQuery {


    public static final Pattern apacheLogRegex = Pattern.compile(
            "(http\\:\\/\\/www\\.the\\-associates\\.co\\.uk)?([^\\?\\&\\,]+)");
    public static final Pattern apacheLogRegex1=Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");

    /**
     * Tracks the total query count and number of aggregate bytes for a particular group.
     */
   /* public static class Stats implements Serializable {

        private final int count;
        private final int numBytes;

        public Stats(int count, int numBytes) {
            this.count = count;
            this.numBytes = numBytes;
        }
        public Stats merge(Stats other) {
            return new Stats(count + other.count, numBytes + other.numBytes);
        }

        public String toString() {
            return String.format("bytes=%s\tn=%s", numBytes, count);
        }
    }*/

   /* public static Tuple3<String, String, String> extractKey(String line) {
        Matcher m = apacheLogRegex.matcher(line);
        if (m.find()) {
            String ip = m.group(1);
            String user = m.group(3);
            String query = m.group(5);
            System.out.println(query);
            if (!user.equalsIgnoreCase("-")) {
                return new Tuple3<>(ip, user, query);
            }
        }
        return new Tuple3<>(null, null, null);
    }
*/
    public static Tuple2<String,Integer> extractKey(String line) {
        Matcher m = apacheLogRegex.matcher(line);
        String query = "";
        if (m.find()) {

            query = m.group();
            //System.out.println(query);
            return new Tuple2(query, 1);
        } else return new Tuple2(query, 0);

    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaLogQuery")
    ma              .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        //JavaRDD<String> dataSet = (args.length == 1) ? jsc.textFile(args[0]) : jsc.parallelize(exampleApacheLogs);

/*        JavaPairRDD<String, Integer> finalresult=result.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);*/
        //JavaRDD<String> distFile = sc.textFile("src/main/resources/access_log");
        JavaRDD<String> distFile = sc.textFile("src/main/resources/access_log").map(
                line -> {

                    String[] parts = line.split(" ");
                    return parts[6];
                });
        //JavaRDD<String> counts = distFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> extracted =
                distFile.mapToPair(s -> extractKey(s)).reduceByKey((a, b) -> a + b);
        JavaPairRDD<Integer, String> swappedPair = extracted.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) item -> item.swap());

        JavaPairRDD<Integer, String> result=swappedPair.sortByKey(false);
        System.out.println(result.first());


        JavaPairRDD<String, Integer> extracted1 =
                distFile.mapToPair(s -> extractKey1(s)).reduceByKey((a, b) -> a + b);
        JavaPairRDD<Integer, String> swappedPair1 = extracted1.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) item -> item.swap());

        JavaPairRDD<Integer, String> result1=swappedPair1.sortByKey(false);
        System.out.println(result1.first());



//extracted.saveAsTextFile("output/");
        // JavaPairRDD<Tuple3<String, String, String>, Stats> counts = extracted.reduceByKey(Stats::merge);


    }

    public static Tuple2<String,Integer> extractKey1(String line) {

        Matcher m = apacheLogRegex1.matcher(line);
        String query = "";
        if (m.find()) {

            query = m.group();
            //System.out.println(query);
            return new Tuple2(query, 1);
        } else return new Tuple2(query, 0);


    }
}