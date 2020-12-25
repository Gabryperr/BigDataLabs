package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    public static void main(String[] args) {

        // The following two lines are used to switch off some verbose log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        String inputPath;
        String outputPath;
        String prefix;

        inputPath = args[0];
        outputPath = args[1];
        prefix = args[2];


        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("Spark Filter");

        // Use the following command to create the SparkConf object if you want to run
        // your application inside Eclipse.
        // Remember to remove .setMaster("local") before running your application on the cluster
        // SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);


        // Read the content of the input file/folder
        // Each element/string of wordFreqRDD corresponds to one line of the input data
        // (i.e, one pair "word\tfreq")
        JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

        // filter the words that contain prefix
        JavaRDD<String> filteredRDD = wordFreqRDD.filter(line -> line.startsWith(prefix));

        // print the number of lines filtered
        long numLines = filteredRDD.count();
        System.out.println("Selected lines: " + numLines);

        // print the max frequency
        int maxFreq = filteredRDD.map(line -> Integer.parseInt(line.split("\\s+")[1]))
                .reduce((value1, value2) -> value1 > value2 ? value1 : value2);
        System.out.println("Max frequency: " + maxFreq);

        // filter the lines with freq > 0.8*maxFreq and keep only the words
        JavaRDD<String> graterRDD = filteredRDD.filter(line ->
                Integer.parseInt(line.split("\\s+")[1]) > 0.8 * maxFreq)
                .map(line -> line.split("\\s+")[0]);

        // print the number of words
        long numWords = graterRDD.count();
        System.out.println("Selected words: " + numWords);

        // store the selected words
        graterRDD.saveAsTextFile(outputPath);

        // Close the Spark context
        sc.close();
    }
}
