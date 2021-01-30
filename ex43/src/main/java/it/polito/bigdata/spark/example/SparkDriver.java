package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class SparkDriver {

    public static void main(String[] args) {

        // The following two lines are used to switch off some verbose log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        String readingsPath;
        String neighborsPath;
        String outputPath;
        String outputPath2;
        String outputPath3;
        int threshold;

        readingsPath = args[0];
        neighborsPath = args[1];
        outputPath = args[2];
        outputPath2 = args[3];
        outputPath3 = args[4];
        threshold = Integer.parseInt(args[5]);


        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("ex36");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the content of the input file/folder
        JavaRDD<String> readingsRDD = sc.textFile(readingsPath).cache();

        // map Readings to criticality pairs, sum the values, select the most critical, sort them
        JavaPairRDD<Double, String> criticalityRDD = readingsRDD.mapToPair(line -> {
            String stationID = line.split(",")[0];
            int freeSlots = Integer.parseInt(line.split(",")[5]);

            return new Tuple2<>(stationID, new Criticality(freeSlots < threshold ? 1 : 0, 1));
        }).reduceByKey(Criticality::sum)
                .mapValues(Criticality::CriticalPercent)
                .filter(pair -> pair._2 > 80)
                .mapToPair(Tuple2::swap)
                .sortByKey(false);

        // store the pairs
        criticalityRDD.saveAsTextFile(outputPath);

        // map values to criticality pairs
        JavaPairRDD<Double, String> timeslotCriticalityRDD = readingsRDD.mapToPair(line -> {
            String stationID = line.split(",")[0];
            int hour = Integer.parseInt(line.split(",")[2]);
            int freeSlots = Integer.parseInt(line.split(",")[5]);

            String timeslot;
            if (hour < 4)
                timeslot = "0-3";
            else if (hour < 8)
                timeslot = "4-7";
            else if (hour < 12)
                timeslot = "8-11";
            else if (hour < 16)
                timeslot = "12-15";
            else if (hour < 20)
                timeslot = "16-19";
            else
                timeslot = "20-23";

            return new Tuple2<>(timeslot + "," + stationID, new Criticality(freeSlots < threshold ? 1 : 0, 1));

        }).reduceByKey(Criticality::sum)
                .mapValues(Criticality::CriticalPercent)
                .filter(pair -> pair._2 > 80)
                .mapToPair(Tuple2::swap)
                .sortByKey(false);

        // store the pairs
        timeslotCriticalityRDD.saveAsTextFile(outputPath2);

        // broadcast the neighbors
        JavaRDD<String> neighborsRDD = sc.textFile(neighborsPath);

        // create an hashmap
        Map<String, List<String>> neighborsDictionary = neighborsRDD.mapToPair(line -> {
            String stationID = line.split(",")[0];
            List<String> neighborsList = Arrays.asList(line.split(",")[1].split("\\s+"));

            return new Tuple2<>(stationID, neighborsList);
        }).collectAsMap();

        final Broadcast<Map<String, List<String>>> dictionaryBroadcast = sc.broadcast(neighborsDictionary);

        JavaRDD<String> fullNeighborsRDD = readingsRDD
                .filter(line -> Integer.parseInt(line.split(",")[5]) == 0)
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String date = fields[1];
                    String hour = fields[2];
                    String minute = fields[3];

                    return new Tuple2<>(date + "," + hour + "," + minute, line);
                })
                .groupByKey()
                .values()
                .flatMap(list -> {
                    List<String> finalList = new ArrayList<>();

                    for (String currValue : list) {
                        String currID = currValue.split(",")[0];
                        List<String> currNeighbors = dictionaryBroadcast.value().get(currID);
                        boolean foundAll = true;
                        for (String neighbor : currNeighbors) {
                            boolean found = false;
                            for (String innerValue : list) {
                                String innerID = innerValue.split(",")[0];
                                if (innerID.equals(neighbor)) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                foundAll = false;
                                break;
                            }
                        }
                        if (foundAll) finalList.add(currValue);
                    }
                    return finalList.iterator();
                });

        // store the RDD
        fullNeighborsRDD.saveAsTextFile(outputPath3);
        // Close the Spark context
        sc.close();
    }
}
