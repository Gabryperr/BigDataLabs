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


        String inputPathCommits;
        String outputPathPart1;
        String outputPathPart2;

        inputPathCommits = args[0];
        outputPathPart1 = args[1];
        outputPathPart2 = args[2];

        //crate conf object
        SparkConf conf = new SparkConf().setAppName("Exam 20200902");

        // create context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Part 1

        //read commits file
        JavaRDD<String> commits = sc.textFile(inputPathCommits);

        // select commits of June 2019
        JavaRDD<String> commitsJune2019 = commits
                .filter(line -> line.split(",")[1].startsWith("2019/06"));

        // compute for each date commits of Apache Spark
        JavaPairRDD<String, Integer> commitsSpark = commitsJune2019
                .filter(line -> line.split(",")[2].equals("Apache Spark"))
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String date = fields[1].split("_")[0];
                    return new Tuple2<>(date, 1);
                })
                .reduceByKey(Integer::sum);

        // compute for each date commits of Apache Flink
        JavaPairRDD<String, Integer> commitsFlink = commitsJune2019
                .filter(line -> line.split(",")[2].equals("Apache Flink"))
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String date = fields[1].split("_")[0];
                    return new Tuple2<>(date, 1);
                })
                .reduceByKey(Integer::sum);

        // for each date compare commits Spark and commits Flink
        JavaPairRDD<String, String> commitsCompare = commitsSpark.join(commitsFlink)
                .mapValues(pair -> {
                    if (pair._1 > pair._2) return "AS";
                    else if (pair._1.equals(pair._2)) return null;
                    else return "AF";
                })
                .filter(pair -> pair._2 != null);

        // store the output of the first part
        commitsCompare.saveAsTextFile(outputPathPart1);

        // Part 2

        // select commits of 2017
        // for each project compute the number of commits per month ((project, month), nCommits )
        // remove pairs with number of commits less than 20
        // for each pair emit a window of three months
        // compute the length of each window
        // select window with length equal to 3
        JavaPairRDD<Tuple2<Integer, String>, Integer> windows = commits
                .filter(line -> line.split(",")[1].startsWith("2017"))
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    int month = Integer.parseInt(fields[1].split("_")[0].split("/")[1]);
                    String project = fields[2];

                    return new Tuple2<>(new Tuple2<>(month, project), 1);
                })
                .reduceByKey(Integer::sum)
                .filter(pair -> pair._2 >= 20)
                .flatMapToPair(pair -> {
                    String project = pair._1._2;
                    int month = pair._1._1;

                    List<Tuple2<Tuple2<Integer, String>, Integer>> window = new ArrayList<>();

                    for (int i = 0; i < 3; i++) {
                        if (month - i > 0) {
                            window.add(new Tuple2<>(new Tuple2<>(month - i, project), 1));
                        }
                    }

                    return window.iterator();
                })
                .reduceByKey(Integer::sum)
                .filter(pair-> pair._2 == 3);

        // store the content of the second part
        windows.keys().saveAsTextFile(outputPathPart2);
        
    }
}
