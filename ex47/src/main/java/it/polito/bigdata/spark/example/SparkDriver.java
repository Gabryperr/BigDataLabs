package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        // filter male people
        // select only name and age+1
        // sort by descending age and ascending name
        Dataset<Row> person = ss.read().format("csv").option("header", true).option("inferSchema", true).load(inputPath);

        // apply the query with dataframes
        Dataset<Row> dfSelectedPerson = person.filter("gender = 'male'")
                .selectExpr("name", "age+1 as age")
                .sort(new Column("age").desc(), new Column("name"));

        // store the dataframe
        dfSelectedPerson.show();
        person.write().format("csv").save(outputPath);

        // apply the query with SQL
        person.createOrReplaceTempView("people");
        Dataset<Row> sqlSelectedPerson = ss
                .sql("SELECT name, age+1 as age FROM people where gender = 'male' ORDER BY age DESC, name");

        // store the dataframe
        sqlSelectedPerson.show();
        sqlSelectedPerson.write().format("csv").save(outputPathSQL);

        // Close the spark session
        ss.stop();
    }
}
