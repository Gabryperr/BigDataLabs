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


        String inputPathServers;
        String inputPathTemperatures;
        String outputPathPart1;
        String outputPathPart2;

        inputPathServers = args[0];
        inputPathTemperatures = args[1];
        outputPathPart1 = args[2];
        outputPathPart2 = args[3];

        //crate conf object
        SparkConf conf = new SparkConf().setAppName("Exam 20200902");

        // create context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Part 1

        // read temperature anomalies and select data from 2010 to 2018
        JavaRDD<String> anomalies10_18 = sc.textFile(inputPathTemperatures)
                .filter(line -> {
                    String date = line.split(",")[1].split("_")[0];

                    return date.compareTo("2010/01/01") >= 0 && date.compareTo("2018/12/31") <= 0;
                });

        // select only temperature above 100 degree
        // for each server and year compute the number of anomalies
        // select only (sID, year) with more than 50 anomalies
        // retrieve only sID and eliminate duplicates
        JavaRDD<String> criticalServers = anomalies10_18
                .filter(line -> Integer.parseInt(line.split(",")[2]) > 100)
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String sID = fields[0];
                    String year = fields[1].split("_")[0].split("/")[0];

                    return new Tuple2<>(new Tuple2<>(sID, year), 1);
                })
                .reduceByKey(Integer::sum)
                .filter(pair -> pair._2 > 1)
                .map(pair -> pair._1._1)
                .distinct();

        // store the output of the first part
        criticalServers.saveAsTextFile(outputPathPart1);

        // Part 2

        //read the file servers and map it to (sID, dataCenterID)
        JavaPairRDD<String, String> servers = sc.textFile(inputPathServers)
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String sID = fields[0];
                    String dataCenterID = fields[2];

                    return new Tuple2<>(sID, dataCenterID);
                });

        // for each server compute the number of anomalies
        // filter to get only servers with more than 10 anomalies
        // join with servers to get dataCenterID
        // map to (dataCenterID, sID)
        JavaPairRDD<String, String> dataCenterToDiscard = anomalies10_18
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String sID = fields[0];

                    return new Tuple2<>(sID, 1);
                })
                .reduceByKey(Integer::sum)
                .filter(pair -> pair._2 > 0)
                .join(servers)
                .mapToPair(pair -> new Tuple2<>(pair._2._2, pair._1));

        // map servers to (dataCenterID, sID)
        // remove data center with servers with more than 10 failures
        // retrieve dataCenterID
        // remove duplicates
        JavaRDD<String> goodDataCenter = servers
                .mapToPair(Tuple2::swap)
                .subtractByKey(dataCenterToDiscard)
                .keys()
                .distinct();


        // store the output of the second part
        goodDataCenter.saveAsTextFile(outputPathPart2);

        // print the number of selected data centers
        System.out.println("servers: " + goodDataCenter.count());
        
    }
}
