package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

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
        SparkConf conf = new SparkConf().setAppName("ex44");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read the input file, emit the values within the windows
        // group
        JavaRDD<String> windowsRDD = sc.textFile(inputPath)
                .flatMapToPair(line -> {
                    List<Tuple2<Integer, Tuple2<Integer, Double>>> readings = new ArrayList<>();

                    String[] fields = line.split(",");
                    int timestamp = Integer.parseInt(fields[0]);
                    double temp = Double.parseDouble(fields[1]);

                    // add the 3 values
                    readings.add(new Tuple2<>(timestamp, new Tuple2<>(timestamp, temp)));
                    readings.add(new Tuple2<>(timestamp - 60, new Tuple2<>(timestamp, temp)));
                    readings.add(new Tuple2<>(timestamp - 120, new Tuple2<>(timestamp, temp)));

                    return readings.iterator();


                })
                .groupByKey()
                .flatMap(value -> {
                    SortedMap<Integer, Double> readings = new TreeMap<>();

                    // store the window
                    for (Tuple2<Integer, Double> tuple : value._2) {
                        readings.put(tuple._1, tuple._2);
                    }

                    List<String> windowList = new ArrayList<>();

                    if (readings.size() == 3) {
                        boolean ordered = true;
                        Double currentTemp = null;
                        for (int key : readings.keySet()) {
                            if (currentTemp != null) {
                                if (readings.get(key) <= currentTemp) {
                                    ordered = false;
                                }
                            }
                            currentTemp = readings.get(key);
                        }
                        if (ordered) {
                            StringBuilder list = new StringBuilder();
                            for (int key : readings.keySet()) {
                                list.append(key).append(",").append(readings.get(key)).append(",");
                            }
                            windowList.add(list.toString());
                        }
                    }

                    return windowList.iterator();
                });

        // store the RDD
        windowsRDD.saveAsTextFile(outputPath);


        // Close the Spark context
        sc.close();
    }
}
