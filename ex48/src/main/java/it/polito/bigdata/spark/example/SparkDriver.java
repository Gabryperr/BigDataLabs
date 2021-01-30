package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;

public class SparkDriver {

    public static void main(String[] args) {

        // The following two lines are used to switch off some verbose log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        String inputPath;
        String outputPath;
        String outputPathSQL;

        inputPath = args[0];
        outputPath = args[1];
        outputPathSQL = args[2];

        // Create a Spark session
        SparkSession ss = SparkSession.builder().appName("ex47").getOrCreate();

        // read the input table
        Dataset<Row> person = ss.read().format("csv").option("header", true).option("inferSchema", true).load(inputPath);

        // apply the query with dataframes
        // group by name
        // count the names
        // compute age average
        Dataset<Row> dfSelectedPerson = person
                .groupBy("name").agg(avg("age").as("avgAge"), count("name").as("occurrences"))
                .filter("occurrences >= 2")
                .select("name", "avgAge");

        // store the dataframe
        dfSelectedPerson.show();
        person.write().format("csv").save(outputPath);

        // apply the query with SQL
        person.createOrReplaceTempView("people");
        Dataset<Row> sqlSelectedPerson = ss
                .sql("SELECT name, avg(age) AS avgAge FROM people GROUP BY name HAVING count(name) >= 2");

        // store the dataframe
        sqlSelectedPerson.show();
        sqlSelectedPerson.write().format("csv").save(outputPathSQL);

        // Close the spark session
        ss.stop();
    }
}
