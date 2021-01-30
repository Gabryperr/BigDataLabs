package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class SparkDriver {

    public static void main(String[] args) {

        // The following two lines are used to switch off some verbose log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        String inputPath;
        //String outputPath;

        inputPath = args[0];
        // outputPath = args[1];


        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("ex32");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the content of the input file/folder
        JavaRDD<String> linesRDD = sc.textFile(inputPath);

        // Extract PM10 value
        JavaRDD<Double> pm10RDD = linesRDD.map(line -> Double.parseDouble(line.split(",")[2]));

        // Take the three biggest values
        List<Double> top3Pm10 = pm10RDD.top(3);

        // print the result
        System.out.println("Top 3:");
        for (Double value : top3Pm10) {
            System.out.println(value);
        }

        // Close the Spark context
        sc.close();
    }
}
