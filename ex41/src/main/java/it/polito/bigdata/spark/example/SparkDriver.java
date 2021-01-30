package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class SparkDriver {

    public static void main(String[] args) {

        // The following two lines are used to switch off some verbose log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        String inputPath;
        String outputPath;
        int k;

        inputPath = args[0];
        outputPath = args[1];
        k = Integer.parseInt(args[2]);


        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("ex36");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the content of the input file/folder
        JavaRDD<String> linesRDD = sc.textFile(inputPath);

        // Map the lines to pairRDD, sum the criticality Dais
        JavaPairRDD<String, Integer> sensorIdCriticality = linesRDD.mapToPair(line -> {
            String sensorID = line.split(",")[0];
            double pm10 = Double.parseDouble(line.split(",")[2]);

            return new Tuple2<>(sensorID, pm10 > 50 ? 1 : 0);
        }).reduceByKey(Integer::sum);

        // Sort the RDD by criticality Days,
        JavaPairRDD<Integer,String> sortedCriticalitySensorID = sensorIdCriticality
                .mapToPair(Tuple2::swap)
                .sortByKey(false);

        // get the top-k values
        List<Tuple2<Integer,String>> topK = sortedCriticalitySensorID.take(k);

        // create an RDD from topK values and store it
        sc.parallelizePairs(topK).saveAsTextFile(outputPath);

        // Close the Spark context
        sc.close();
    }
}
