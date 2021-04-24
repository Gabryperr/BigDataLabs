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
        String inputPathPatchesServers;
        String outputPathPart1;
        String outputPathPart2;

        inputPathServers = args[0];
        inputPathPatchesServers = args[1];
        outputPathPart1 = args[2];
        outputPathPart2 = args[3];

        //crate conf object
        SparkConf conf = new SparkConf().setAppName("Exam 20200902");

        // create context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Part 1

        // read the servers file
        JavaPairRDD<String, String> servers = sc.textFile(inputPathServers)
                .mapToPair(line -> {
                    String[] fields = line.split(",");

                    String sID = fields[0];
                    String model = fields[1];

                    return new Tuple2<>(sID, model);
                }).cache();

        // read the patched servers file
        JavaRDD<String> patchesServers = sc.textFile(inputPathPatchesServers).cache();

        // select 2018 patches
        // for each SID compute the number of patches
        JavaPairRDD<String, Integer> patches2018 = patchesServers
                .filter(line -> line.split(",")[2].startsWith("2018"))
                .mapToPair(line -> new Tuple2<>(line.split(",")[0], 1))
                .reduceByKey(Integer::sum);

        // select 2019 patches
        // for each SID compute the number of patches
        JavaPairRDD<String, Integer> patches2019 = patchesServers
                .filter(line -> line.split(",")[2].startsWith("2019"))
                .mapToPair(line -> new Tuple2<>(line.split(",")[0], 1))
                .reduceByKey(Integer::sum);

        // join the content of 2018 and 2019
        // select only ones with decreased patch
        JavaPairRDD<String, Tuple2<Integer, Integer>> decreasePatch = patches2019.join(patches2018)
                .filter(pair -> pair._2._1 < 0.5 * pair._2._2);

        // join with server file to get the model
        JavaPairRDD<String, String> decreaseModel = servers.join(decreasePatch)
                .mapValues(value -> value._1);

        // store the content of the first part
        decreaseModel.saveAsTextFile(outputPathPart1);


        //Part 2

        // for each server and date compute the patch per day
        // select sid with patches per day greater than 1
        // map the result to (sid,"")
        JavaPairRDD<String,String> sidToDiscard = patchesServers
                .mapToPair(line -> {
                    String[] fields = line.split(",");

                    String sID = fields[0];
                    String date = fields[2];

                    return new Tuple2<>(new Tuple2<>(sID, date), 1);
                })
                .reduceByKey(Integer::sum)
                .filter(pair -> pair._2 > 1)
                .mapToPair(pair-> new Tuple2<>(pair._1._1,""));

        // subtract from (sid,model) sidToDiscard
        JavaPairRDD<String,String> onePatchModel = servers.subtractByKey(sidToDiscard);

        // store the content of the second part
        onePatchModel.saveAsTextFile(outputPathPart2);

        // print the number of distinct models
        System.out.println("Distinct Models:" + onePatchModel.values().distinct().count());

    }
}
