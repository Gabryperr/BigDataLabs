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

        // map the values to pair (sensorID,pm10)
        JavaPairRDD<String, Double> sensorIDAndValue = linesRDD.mapToPair(line -> {
            String sensorID = line.split(",")[0];
            Double pm10 = Double.parseDouble(line.split(",")[2]);

            return new Tuple2<>(sensorID, pm10);
        });

        // compute the maximum for each sensorID
        JavaPairRDD<String, Double> maxPm10Values = sensorIDAndValue
                .reduceByKey((value1, value2) -> value1 > value2 ? value1 : value2);

        // store the RDD
        maxPm10Values.saveAsTextFile(outputPath);


        // Close the Spark context
        sc.close();
    }
}
