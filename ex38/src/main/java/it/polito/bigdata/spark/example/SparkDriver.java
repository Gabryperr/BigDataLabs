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
        JavaPairRDD<String, Integer> sensorIDAndValue = linesRDD.mapToPair(line -> {
            String sensorID = line.split(",")[0];
            double pm10 = Double.parseDouble(line.split(",")[2]);

            return new Tuple2<>(sensorID, pm10 > 50 ? 1 : 0);
        });

        // compute the critical readings for each sensor
        JavaPairRDD<String, Integer> criticalReadings = sensorIDAndValue.reduceByKey(Integer::sum);

        // get the ID with at least 2 readings
        JavaPairRDD<String,Integer> atLeast2 = criticalReadings.filter(pair -> pair._2 > 1);

        // store the RDD
        atLeast2.saveAsTextFile(outputPath);


        // Close the Spark context
        sc.close();
    }
}
