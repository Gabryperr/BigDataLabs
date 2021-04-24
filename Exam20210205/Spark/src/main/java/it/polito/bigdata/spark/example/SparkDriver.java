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


        String inFailures;
        String inProdPlants;
        String inRobots;
        String outputPathPart1;
        String outputPathPart2;

        inFailures = args[0];
        inProdPlants = args[1];
        inRobots = args[2];
        outputPathPart1 = args[3];
        outputPathPart2 = args[4];

        //crate conf object
        SparkConf conf = new SparkConf().setAppName("Exam 20200917");

        // create context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Part 1

        // select failures of 2020
        JavaRDD<String> failures2020 = sc.textFile(inFailures)
                .filter(line -> line.split(",")[2].startsWith("2020"))
                .cache();

        //read the robots file and map it to (rID, plantID)
        JavaPairRDD<String, String> robots = sc.textFile(inRobots)
                .mapToPair(line -> {
                    String[] fields = line.split(",");

                    String rID = fields[0];
                    String plantID = fields[1];

                    return new Tuple2<>(rID, plantID);
                }).cache();

        // for each robot compute the number of failures
        // filter to get robots with at least 50 failures
        // join with robots file to get (rID, (failures, plantID) )
        // take only the distinct plantID
        JavaRDD<String> plantWith50Failures = failures2020
                .mapToPair(line -> new Tuple2<>(line.split(",")[0], 1))
                .reduceByKey(Integer::sum)
                .filter(pair -> pair._2 >= 10)
                .join(robots)
                .map(tuple -> tuple._2._2)
                .distinct();

        // store the output of the first part
        plantWith50Failures.saveAsTextFile(outputPathPart1);

        // Part 2

        // select distinct robot that had failure in 2020
        // join with robots file to get the plantID
        // map each pair to (plantID, 1)
        // sum by key
        JavaPairRDD<String,Integer> failedPlants = failures2020
                .mapToPair(line -> new Tuple2<>(line.split(",")[0], 1))
                .distinct()
                .join(robots)
                .mapToPair(pair-> new Tuple2<>(pair._2._2,pair._2._1))
                .reduceByKey(Integer::sum);

        // map the robots file to (plantID,0)
        // remove duplicates
        // subtract the filed plants
        JavaPairRDD<String,Integer> nonFailedPlants = robots
                .mapToPair(pair-> new Tuple2<>(pair._2,0))
                .distinct()
                .subtractByKey(failedPlants);

        // union failed and non failed plants and save the output
        failedPlants.union(nonFailedPlants).saveAsTextFile(outputPathPart2);

    }
}
