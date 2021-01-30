package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

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

        // create nameSurname function
        ss.udf().register("nameSurname", (String name, String surname) -> name + " " + surname, DataTypes.StringType);

        // read the input table
        Dataset<Row> person = ss.read().format("csv").option("header", true).option("inferSchema", true).load(inputPath);

        // apply the query with dataframes
        // select nameSurname
        Dataset<Row> dfSelectedPerson = person.selectExpr("nameSurname(name,surname) as name_surname");

        // store the dataframe
        dfSelectedPerson.show();
        person.write().format("csv").save(outputPath);

        // apply the query with SQL
        person.createOrReplaceTempView("people");
        Dataset<Row> sqlSelectedPerson = ss
                .sql("SELECT nameSurname(name,surname) AS name_surname FROM people");

        // store the dataframe
        sqlSelectedPerson.show();
        sqlSelectedPerson.write().format("csv").save(outputPathSQL);

        // Close the spark session
        ss.stop();
    }
}
