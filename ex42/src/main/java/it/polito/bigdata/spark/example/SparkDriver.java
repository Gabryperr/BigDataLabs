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


        String questionPath;
        String answerPath;
        String outputPath;

        questionPath = args[0];
        answerPath = args[1];
        outputPath = args[2];


        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("ex36");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the content of the input file/folder
        JavaRDD<String> questionsRDD = sc.textFile(questionPath);
        JavaRDD<String> answersRDD = sc.textFile(answerPath);

        // map to pairs the answers/questions
        JavaPairRDD<String, String> mappedQuestRDD = questionsRDD.mapToPair(line -> {
            String questionID = line.split(",", 3)[0];
            String text = line.split(",", 3)[2];

            return new Tuple2<>(questionID, text);
        });
        JavaPairRDD<String, String> mappedAnswerRDD = answersRDD.mapToPair(line -> {
            String questionID = line.split(",", 4)[1];
            String text = line.split(",", 4)[3];

            return new Tuple2<>(questionID, text);
        });

        // co-group questions and answers
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> groupedRDD = mappedQuestRDD.cogroup(mappedAnswerRDD);

        // Store the RDD
        groupedRDD.saveAsTextFile(outputPath);


        // Close the Spark context
        sc.close();
    }
}
