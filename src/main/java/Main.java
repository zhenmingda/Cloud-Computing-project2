import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import scala.Tuple2;
import org.apache.spark.sql.*;


public class Main {

    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("src/main/resources/user_artists.dat")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("\\s+");
                    Person person = new Person();
                    person.setUserID(parts[0]);
                    person.setArtistID(parts[1]);
                    person.setWeight(Integer.parseInt(parts[2]));
                    return person;
                });

// Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
// Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

// SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("SELECT ArtistID, " +
                "SUM(weight) as TotalWeight FROM people group by ArtistID order by SUM(weight) DESC");
        teenagersDF.show();


    }

}