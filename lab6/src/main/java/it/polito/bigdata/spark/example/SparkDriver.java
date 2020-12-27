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
        SparkConf conf = new SparkConf().setAppName("SparkPeopleAlsoLike");

        // Use the following command to create the SparkConf object if you want to run
        // your application inside Eclipse.
        // Remember to remove .setMaster("local") before running your application on the cluster
        // SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);


        // Read the content of the input file
        JavaRDD<String> inputRDD = sc.textFile(inputPath);

        // extract ProductId, UserID; remove header; remove duplicates
        JavaPairRDD<String, String> distinctRDD = inputRDD.mapToPair(line -> {
            String[] fields = line.split(",");
            return new Tuple2<>(fields[2], fields[1]);
        }).filter(pair -> !pair._1().equals("UserId")).distinct();

        // create a list of products for each reviewer
        JavaPairRDD<String, Iterable<String>> groupedRDD = distinctRDD.groupByKey();

        // create the product pair from each list
        JavaPairRDD<String, Integer> productPairsRDD = groupedRDD.flatMapToPair(userAndList -> {

            // create the pairs
            ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
            for (String p1 : userAndList._2()) {
                for (String p2 : userAndList._2()) {
                    if (p1.compareTo(p2) > 0) {
                        pairs.add(new Tuple2<>((p1 + " " + p2), 1));
                    }
                }
            }

            // return the iterator
            return pairs.iterator();
        });

        // count the frequency of each pair, remove pairs with frequency less than 2
        JavaPairRDD<String, Integer> countedPairsRDD = productPairsRDD.reduceByKey(Integer::sum)
                .filter(pair -> pair._2() > 1);

        // Invert key and value and sort the products
        JavaPairRDD<Integer, String> sortedPairsRDD = countedPairsRDD
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                .sortByKey(false);

        // Store the result in the output folder
        sortedPairsRDD.saveAsTextFile(outputPath);

        // take the 10 most frequent pairs
        List<Tuple2<Integer, String>> topPairs = sortedPairsRDD.take(10);
        topPairs.forEach(System.out::println);

        // Close the Spark context
        sc.close();
    }
}
