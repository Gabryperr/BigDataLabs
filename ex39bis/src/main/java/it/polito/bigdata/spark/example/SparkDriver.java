package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

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

        // Map the lines to pairRDD
        JavaPairRDD<String, String> sensorIdDate = linesRDD.mapToPair(line -> {
            String sensorID = line.split(",")[0];
            String date = line.split(",")[1];
            double pm10 = Double.parseDouble(line.split(",")[2]);

            return new Tuple2<>(sensorID, pm10 > 50 ? date : "");
        });

        // aggregate by sensorID
        JavaPairRDD<String, Iterable<String>> sensorIdAggregated = sensorIdDate.groupByKey();

        // remove empty strings
        JavaPairRDD<String, List<String>> filteredLists = sensorIdAggregated.mapValues(values -> {
            List<String> outList = new ArrayList<>();

            for (String value : values) {
                if (!value.equals("")) {
                    outList.add(value);
                }
            }

            return outList;
        });

        // store the RDD
        filteredLists.saveAsTextFile(outputPath);


        // Close the Spark context
        sc.close();
    }
}
