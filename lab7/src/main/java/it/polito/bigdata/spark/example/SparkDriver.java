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

        String registerFile;
        String stationsFile;
        double threshold;
        String outputFolder;

        registerFile = args[0];
        stationsFile = args[1];
        threshold = Double.parseDouble(args[2]);
        outputFolder = args[3];

        // Create a configuration object and set the name of the application
        SparkConf conf = new SparkConf().setAppName("SparkBike");

        // Use the following command to create the SparkConf object if you want to run
        // your application inside Eclipse.
        // SparkConf conf=new SparkConf().setAppName("Spark Lab #7").setMaster("local");

        // Create a Spark Context object
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read data
        JavaRDD<String> registerDataRDD = sc.textFile(registerFile);

        // remove from register data file header and wrong lines
        JavaRDD<String> cleanRegDataRDD = registerDataRDD.filter(line -> {
            // remove the header
            if (line.startsWith("station")) return false;

            // split line and check wrong lines
            String[] fields = line.split("\\s+");
            int usedSlots = Integer.parseInt(fields[3]);
            int freeSlots = Integer.parseInt(fields[4]);
            return (usedSlots != 0) || (freeSlots != 0);

        });

        // for each line emits a pair with indications on criticality
        // Pair: (StationID,weekDay,hour - CriticalTool)
        JavaPairRDD<String, CriticalTool> regDataPairsRDD = cleanRegDataRDD.mapToPair(line -> {
            // split line and extract fields
            String[] fields = line.split("\\s+");

            String stationID = fields[0];
            String weekDay = DateTool.DayOfTheWeek(fields[1]);
            String hour = fields[2].split(":")[0];
            int freeSlots = Integer.parseInt(fields[4]);

            // return the correct object in case of criticality or not
            if (freeSlots == 0) {
                return new Tuple2<>(stationID + "," + weekDay + "," + hour,
                        new CriticalTool(1, 1));
            } else {
                return new Tuple2<>(stationID + "," + weekDay + "," + hour,
                        new CriticalTool(1, 0));

            }
        });

        // compute the criticality for each Station - Timeslot
        // Pair: (StationID,weekDay,hour - criticality)
        JavaPairRDD<String, Double> criticalityPairsRDD = regDataPairsRDD
                .reduceByKey(CriticalTool::sum)
                .mapValues(CriticalTool::getCriticality);

        // filter the values with criticality above threshold
        // Pair: (StationID,weekDay,hour - criticality)
        JavaPairRDD<String, Double> filtCriticPairsRDD = criticalityPairsRDD
                .filter(pair -> pair._2() >= threshold);

        // select only the most critical timeslot for each station
        // Pair: (StationID - (weekDay,hour - criticality))
        JavaPairRDD<String, Tuple2<String, Double>> topCriticalTimeslotsRDD = filtCriticPairsRDD
                .mapToPair(pair -> {
                    // new key is stationID
                    String key = pair._1().split(",", 2)[0];

                    // new value is the time slot and the criticality value
                    String timeSlot = pair._1().split(",", 2)[1];
                    double criticality = pair._2();
                    Tuple2<String, Double> value = new Tuple2<>(timeSlot, criticality);

                    return new Tuple2<>(key, value);
                })
                .reduceByKey((p1, p2) -> {
                    // extract fields from both pairs
                    double critic1 = p1._2();
                    String weekDay1 = p1._1().split(",")[0];
                    int hour1 = Integer.parseInt(p1._1().split(",")[1]);

                    double critic2 = p2._2();
                    String weekDay2 = p2._1().split(",")[0];
                    int hour2 = Integer.parseInt(p2._1().split(",")[1]);

                    // return the one with higher criticality
                    if (critic1 > critic2) return new Tuple2<>(p1._1(), p1._2());
                    else if (critic1 == critic2) {
                        // return the one with earliest hour
                        if (hour1 < hour2) return new Tuple2<>(p1._1(), p1._2());
                        else if (hour1 == hour2) {
                            // return looking at the lexicographical order of the weekDay
                            if (weekDay1.compareTo(weekDay2) < 0) return new Tuple2<>(p1._1(), p1._2());
                        }
                    }
                    return new Tuple2<>(p2._1(), p2._2());
                });

        // read the station file, remove the header
        JavaRDD<String> stationsRDD = sc.textFile(stationsFile)
                .filter(line -> !line.startsWith("id"));

        // create the station pairs with longitude and latitude
        // Pair: (StationID - longitude,latitude)
        JavaPairRDD<String, String> stationsPairsRdd = stationsRDD.mapToPair(line -> {
            // extract fields
            String[] fields = line.split("\\s+");
            String stationID = fields[0];
            String longLat = fields[1] + "," + fields[2];

            //return the pair
            return new Tuple2<>(stationID, longLat);
        });

        // Store in resultKML one String, representing a KML marker, for each station
        // with a critical timeslot
        // Input Pair: (StationID - ((weekDay,hour - criticality) - longitude,latitude))
        JavaRDD<String> resultKML = topCriticalTimeslotsRDD.join(stationsPairsRdd)
                .map(pair -> {
                    // extract all the fields
                    String stationID = pair._1();
                    String weekDay = pair._2()._1()._1().split(",")[0];
                    int hour = Integer.parseInt(pair._2()._1()._1().split(",")[1]);
                    double criticality = pair._2()._1()._2();
                    String longLat = pair._2()._2();

                    // construct the KML string
                    return "<Placemark><name>" + stationID + "</name><ExtendedData>" +
                            "<Data name=\"DayWeek\"><value>" + weekDay + "</value></Data>" +
                            "<Data name=\"Hour\"><value>" + hour + "</value></Data>" +
                            "<Data name=\"Criticality\"><value>" + criticality + "</value></Data>" +
                            "</ExtendedData><Point><coordinates>" + longLat +
                            "</coordinates></Point></Placemark>";

                });

        // Invoke coalesce(1) to store all data inside one single partition/i.e., in one single output part file
        resultKML.coalesce(1).saveAsTextFile(outputFolder);

        // Close the Spark context
        sc.close();
    }
}
