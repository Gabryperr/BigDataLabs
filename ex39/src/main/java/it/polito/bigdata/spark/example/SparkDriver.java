package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDriver {

    public static void main(String[] args) {

        // The following two lines are used to switch off some verbose log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        String inputPath;
        String outputPath;

        inputPath = args[0];
        outputPath = args[1];


        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("ex36");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the content of the input file/folder
        JavaRDD<String> linesRDD = sc.textFile(inputPath);

        // Filter the lines
        JavaRDD<String> filteredLinesRDD = linesRDD.filter(line -> Double.parseDouble(line.split(",")[2]) > 50);

        // Map the lines to pairRDD
        JavaPairRDD<String, String> sensorIdDate = filteredLinesRDD.mapToPair(line->{
            String sensorID = line.split(",")[0];
            String date = line.split(",")[1];

            return new Tuple2<>(sensorID,date);
        });

        // aggregate by sensorID
        JavaPairRDD<String, Iterable<String>> sensorIdAggregated = sensorIdDate.groupByKey();

        // store the RDD
        sensorIdAggregated.saveAsTextFile(outputPath);


        // Close the Spark context
        sc.close();
    }
}
