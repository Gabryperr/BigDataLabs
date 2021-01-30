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


        String inUsers;
        String inMovies;
        String inWatchedMovies;
        String outputPathPart1;
        String outputPathPart2;

        inUsers = args[0];
        inMovies = args[1];
        inWatchedMovies = args[2];
        outputPathPart1 = args[3];
        outputPathPart2 = args[4];

        //crate conf object
        SparkConf conf = new SparkConf().setAppName("Exam 20200917");

        // create context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Part1
        // read the file
        // filter the file to get the last 5 years
        JavaRDD<String> lastFiveYears = sc.textFile(inWatchedMovies)
                .filter(line -> {
                    String startTime = line.split(",")[2];
                    String date = startTime.split("_")[0];
                    return date.compareTo("2015/09/17") >= 0 && date.compareTo("2020/09/16") <= 0;
                }).cache();

        // compute the total views per movie and year ((movieID,year), nViews)
        // take the movies watched at list 1000 times in that year
        // map the result to (movieID,year)
        JavaPairRDD<String, String> topWatchedPerYear = lastFiveYears.mapToPair(line -> {
            String[] fields = line.split(",");
            String movieID = fields[1];
            String year = fields[2].split("/")[0];
            return new Tuple2<>(new Tuple2<>(movieID, year), 1);

        }).reduceByKey(Integer::sum).filter(pair -> pair._2 >= 4).mapToPair(pair -> {
            String movieID = pair._1._1;
            String year = pair._1._2;
            return new Tuple2<>(movieID, year);
        });

        // get the distinct years for each movie
        // compute how many years a movie have been watched
        // filter to obtain only the movies watched one year
        JavaPairRDD<String, Integer> oneYearWatched = lastFiveYears.mapToPair(line -> {
            String[] fields = line.split(",");
            String movieID = fields[1];
            String year = fields[2].split("/")[0];
            return new Tuple2<>(movieID, year);
        }).distinct().mapValues(year -> 1).reduceByKey(Integer::sum).filter(pair -> pair._2 == 1);

        // Join the content of the two RDD to get top movies watched only in one year
        JavaPairRDD<String, String> topSingleYear = topWatchedPerYear.join(oneYearWatched)
                .mapToPair(pair -> {
                    String movieID = pair._1;
                    String year = pair._2._1;
                    return new Tuple2<>(movieID, year);
                });

        // Store the RDD
        topSingleYear.saveAsTextFile(outputPathPart1);

        // Part2

        // get the distinct user watching a film in a year ((movieID,year),userID)
        // compute the number of distinct views for each movie and year ((movieID,year), distinctViews)
        // compute the top film for each year (year,(movieID,distinctViews))
        // compute how many years a film has been the top
        // select only the ones with at least 2 years
        JavaPairRDD<String, Integer> topFilms = lastFiveYears
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String userID = fields[0];
                    String movieID = fields[1];
                    String year = fields[2].split("/")[0];
                    return new Tuple2<>(new Tuple2<>(movieID, year), userID);
                }).distinct()
                .mapValues(userID -> 1)
                .reduceByKey(Integer::sum)
                .mapToPair(pair -> {
                    String year = pair._1._2;
                    String movieID = pair._1._1;
                    int views = pair._2;
                    return new Tuple2<>(year, new Tuple2<>(movieID, views));
                })
                .groupByKey()
                .flatMapToPair(pair -> {
                    int topViews = 0;
                    List<Tuple2<String, Integer>> topList = new ArrayList<>();

                    for (Tuple2<String, Integer> tuple : pair._2) {
                        if (tuple._2 > topViews) {
                            topViews = tuple._2;
                            topList.clear();
                            topList.add(new Tuple2<>(tuple._1, 1));
                        } else if (tuple._2 == topViews)
                            topList.add(new Tuple2<>(tuple._1, 1));
                    }
                    return topList.iterator();

                })
                .reduceByKey(Integer::sum)
                .filter(pair -> pair._2 >= 2);

        // store the movie ID
        topFilms.keys().saveAsTextFile(outputPathPart2);

    }
}
