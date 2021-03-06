package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkDriver {

    public static void main(String[] args) {

        // The following two lines are used to switch off some verbose log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        String watchedMoviesPath;
        String preferencesPath;
        String moviesPath;
        String outputPath;
        double threshold;

        watchedMoviesPath = args[0];
        preferencesPath = args[1];
        moviesPath = args[2];
        outputPath = args[3];
        threshold = Double.parseDouble(args[4]);


        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("ex44");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the content of the input file/folder
        //map to pair (movieID, userID)
        JavaPairRDD<String, String> watchedMovieRDD = sc.textFile(watchedMoviesPath).mapToPair(line -> {
            String[] fields = line.split(",");
            String userID = fields[0];
            String movieID = fields[1];
            return new Tuple2<>(movieID, userID);
        });

        //map to pair (movieID, genre)
        JavaPairRDD<String, String> moviesRDD = sc.textFile(moviesPath).mapToPair(line -> {
            String[] fields = line.split(",");
            String movieID = fields[0];
            String genre = fields[2];
            return new Tuple2<>(movieID, genre);
        });

        // join the previous two to obtain (userID, genre)
        JavaPairRDD<String, String> userIdGenre = watchedMovieRDD.join(moviesRDD).mapToPair(joinedPair -> {
            String userID = joinedPair._2._1;
            String genre = joinedPair._2._2;
            return new Tuple2<>(userID, genre);
        });

        // create list of preference
        JavaPairRDD<String, Iterable<String>> listPreferencesRDD = sc.textFile(preferencesPath)
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String userID = fields[0];
                    String genre = fields[1];
                    return new Tuple2<>(userID, genre);
                })
                .groupByKey();

        // Join userId watched genre with list of preferences
        // detect the misleading views
        // compute misleading percentage
        // filter above threshold
        // retrieve userID
        JavaPairRDD<String, Double> misleadingUsersRDD = userIdGenre
                .join(listPreferencesRDD)
                .mapToPair(view -> {
                    String userId = view._1;
                    String watchedGenre = view._2._1;

                    List<String> likeList = new ArrayList<>();
                    // store the list
                    for (String value : view._2._2) {
                        likeList.add(value);
                    }

                    return new Tuple2<>(userId, new Misleading(likeList.contains(watchedGenre) ? 0 : 1, 1));
                })
                .reduceByKey(Misleading::sum)
                .mapValues(Misleading::CriticalPercent)
                .filter(pair -> pair._2 > threshold);

        // count for each user the movie genre watched
        JavaPairRDD<String, Tuple2<String, Integer>> countedGenreRDD = userIdGenre.mapToPair(pair -> {
            String userID = pair._1;
            String genre = pair._2;

            return new Tuple2<>(new Tuple2<>(userID, genre), 1);
        }).reduceByKey(Integer::sum)
                .mapToPair(pair -> {
                    String userID = pair._1._1;
                    String genre = pair._1._2;
                    int nTimes = pair._2;
                    return new Tuple2<>(userID, new Tuple2<>(genre, nTimes));
                });

        // Join the misleading userID with their list of preferences
        // Join with the counted Genre
        JavaPairRDD<String, Tuple2<Tuple2<Double, Iterable<String>>, Tuple2<String, Integer>>> misleadingListRDD = misleadingUsersRDD
                .join(listPreferencesRDD)
                .join(countedGenreRDD);

        // filter the list
        JavaPairRDD<String,String> filteredListRDD = misleadingListRDD.flatMapValues(value -> {
            String genre = value._2._1;
            int nTimes = value._2._2;
            List<String> likedGenre = new ArrayList<>();
            for (String v : value._1._2) {
                likedGenre.add(v);
            }

            List<String> newGenre = new ArrayList<>();
            if(!likedGenre.contains(genre) && nTimes >= 5){
                newGenre.add(genre);
            }
            
            return newGenre;
        });

        // store the RDD
        filteredListRDD.saveAsTextFile(outputPath);


        // Close the Spark context
        sc.close();
    }
}
