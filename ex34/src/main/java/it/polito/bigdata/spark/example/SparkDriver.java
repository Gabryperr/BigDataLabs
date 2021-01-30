package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
        SparkConf conf = new SparkConf().setAppName("ex32");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the content of the input file/folder
        JavaRDD<String> linesRDD = sc.textFile(inputPath).cache();

        // Extract PM10 value
        JavaRDD<Double> pm10RDD = linesRDD.map(line -> Double.parseDouble(line.split(",")[2]));

        // compute the max value
        double maxPm10 = pm10RDD.reduce((value1, value2) -> value1 > value2 ? value1 : value2);

        // take the lines associated with max value
        JavaRDD<String> maxLinesRDD = linesRDD.filter(line -> Double.parseDouble(line.split(",")[2]) == maxPm10);

        // store the lines
        maxLinesRDD.saveAsTextFile(outputPath);

        // Close the Spark context
        sc.close();
    }
}
