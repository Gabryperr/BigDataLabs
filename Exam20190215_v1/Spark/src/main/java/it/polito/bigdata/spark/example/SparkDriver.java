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


        String inputPathPOI;
        String outputPathPart1;
        String outputPathPart2;

        inputPathPOI = args[0];
        outputPathPart1 = args[1];
        outputPathPart2 = args[2];

        //crate conf object
        SparkConf conf = new SparkConf().setAppName("Exam 20200902");

        // create context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Part 1

        // read POI files and select only italian POIs
        JavaRDD<String> italianPOI = sc.textFile(inputPathPOI)
                .filter(line -> line.split(",")[4].equals("Italy"));

        // select only POI with taxi
        // extract the city
        JavaRDD<String> taxiCity = italianPOI
                .filter(line -> line.split(",")[6].equals("taxi"))
                .map(line -> line.split(",")[3]);

        // select only POI with buses
        // extract the city
        JavaRDD<String> busesCity = italianPOI
                .filter(line -> line.split(",")[6].equals("busstop"))
                .map(line -> line.split(",")[3]);

        // remove from city with Taxi city with buses
        JavaRDD<String> taxiNoBusesCity = taxiCity
                .subtract(busesCity)
                .distinct();

        // store the output of the first part
        taxiNoBusesCity.saveAsTextFile(outputPathPart1);

        // Part 2

        // compute the number of museum per city
        JavaPairRDD<String, Integer> museumPerCity = italianPOI
                .filter(line -> line.split(",")[6].equals("museum"))
                .mapToPair(line -> {
                    String city = line.split(",")[3];
                    return new Tuple2<>(city, 1);
                })
                .reduceByKey(Integer::sum);

        // compute the average number of museum
        int totMuseum = museumPerCity.values().reduce(Integer::sum);
        long totCity = museumPerCity.keys().count();
        Double avgMuseum = (double) totMuseum / (double) totCity;

        // select city with number of museum above the average
        JavaPairRDD<String,Integer> manyMuseum = museumPerCity.
                filter(pair-> pair._2 > avgMuseum);

        // Store the second part output
        manyMuseum.keys().saveAsTextFile(outputPathPart2);
    }
}
