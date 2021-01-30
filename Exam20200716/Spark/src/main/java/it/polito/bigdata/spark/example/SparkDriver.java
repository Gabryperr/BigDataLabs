package it.polito.bigdata.spark.example;

import org.apache.jute.compiler.JField;
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


        String inputPathBooks;
        String inputPathPurchases;
        String outputPathPart1;
        String outputPathPart2;

        inputPathBooks = args[0];
        inputPathPurchases = args[1];
        outputPathPart1 = args[2];
        outputPathPart2 = args[3];

        //crate conf object
        SparkConf conf = new SparkConf().setAppName("Exam 20200917");

        // create context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Part 1

        //read the file
        // select only 2018 lines
        JavaRDD<String> purchases2018 = sc.textFile(inputPathPurchases)
                .filter(line -> {
                    String[] fields = line.split(",");
                    String date = fields[2];
                    return date.startsWith("2018");
                });


        // compute the daily purchases of each book ((bookID,date), dailyPurchases)
        JavaPairRDD<Tuple2<String, String>, Integer> dailyPurchases = purchases2018
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String bookID = fields[1];
                    String date = fields[2];
                    return new Tuple2<>(new Tuple2<>(bookID, date), 1);
                })
                .reduceByKey(Integer::sum).cache();

        // for each book compute the max of daily Purchases (bookID, maxDailyPurchases)
        JavaPairRDD<String, Integer> maxDailyPurchases = dailyPurchases
                .mapToPair(pair -> {
                    String bookID = pair._1._1;
                    int purchases = pair._2;
                    return new Tuple2<>(bookID, purchases);
                })
                .reduceByKey(Integer::max);

        // Store the output
        maxDailyPurchases.saveAsTextFile(outputPathPart1);


        //Part 2

        // compute for each book the total purchases for year 2018
        JavaPairRDD<String, Integer> totalPurchases = dailyPurchases
                .mapToPair(pair -> {
                    String bookID = pair._1._1;
                    int purchases = pair._2;
                    return new Tuple2<>(bookID, purchases);
                })
                .reduceByKey(Integer::sum);

        // Join totalPurchases and dailyPurchases for each Book (bookID, ((date,dailyPurchases), totalPurchases) )
        // select only days with purchases > 10%
        // for each book emit 3 days for each selected day ((bookID,date),1)
        // select only windows of 3 days

        JavaPairRDD<Tuple2<String, String>, Integer> windows = dailyPurchases
                .mapToPair(pair -> {
                    String bookID = pair._1._1;
                    String date = pair._1._2;
                    int purchases = pair._2;
                    return new Tuple2<>(bookID, new Tuple2<>(date, purchases));
                })
                .join(totalPurchases)
                .filter(pair -> {
                    int daily = pair._2._1._2;
                    int total = pair._2._2;
                    return daily > 0.1 * total;
                })
                .flatMapToPair(pair -> {
                    String bookID = pair._1;
                    String date = pair._2._1._1;

                    List<Tuple2<Tuple2<String, String>, Integer>> valueList = new ArrayList<>();
                    for (int i = 0; i < 3; i++) {
                        valueList.add(new Tuple2<>(new Tuple2<>(bookID, DateTool.previousDeltaDate(date, i)), 1));
                    }
                    return valueList.iterator();
                })
                .reduceByKey(Integer::sum)
                .filter(pair -> pair._2 == 3);

        // Store (bookID,startDay)
        windows.keys().saveAsTextFile(outputPathPart2
        );


    }
}
