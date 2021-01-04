package it.polito.bigdata.spark.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.avg;

public class SparkDriver {

    public static void main(String[] args) {

        String registerFile;
        String stationFile;
        double threshold;
        String outputFolder;

        registerFile = args[0];
        stationFile = args[1];
        threshold = Double.parseDouble(args[2]);
        outputFolder = args[3];

        // Create a Spark Session object and set the name of the application
        SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

        // Invoke .master("local") to execute tha application locally inside Eclipse
        // SparkSession ss = SparkSession.builder().master("local").appName("Spark Lab #8 - Template").getOrCreate();

        // read the register file
        Dataset<Row> inputRecords = ss.read().format("csv").option("header", true)
                .option("inferSchema", true).option("delimiter", "\\t").load(registerFile);

        // remove lines with used_slots = 0 and free_slots = 0
        Dataset<Row> cleanRecords = inputRecords.filter("used_slots > 0 OR free_slots > 0");

        // create user defined function to get dayOfTheWeek from timestamp
        ss.udf().register("DayOfTheWeek", (Object timestamp) -> {
                    if (timestamp instanceof String)
                        return DateTool.DayOfTheWeek(((String) timestamp).split("\\s+")[0]);
                    else
                        return DateTool.DayOfTheWeek((Timestamp) timestamp);
                },
                DataTypes.StringType);

        ss.udf().register("Criticality",
                (Integer freeSlots) -> freeSlots > 0 ? 0 : 1,
                DataTypes.IntegerType);

        // transform records extracting dayOfTheWeek, hour and criticality value
        Dataset<Row> transformedRecords = cleanRecords.selectExpr(
                "station",
                "DayOfTheWeek(timestamp) as dayofweek",
                "hour(timestamp) as hour",
                "Criticality(free_slots) as criticalityBool");

        // compute the criticality value for each slot
        Dataset<Row> stationCriticality = transformedRecords
                .groupBy("station", "dayofweek", "hour")
                .agg(avg("criticalityBool").as("criticality"));

        // select only rows with criticality grater than threshold
        Dataset<Row> filtStatCritic = stationCriticality.filter("criticality > " + threshold);

        // read the station file and remove station names
        Dataset<Row> stations = ss.read().format("csv").option("header", true)
                .option("inferSchema", true).option("delimiter", "\\t").load(stationFile)
                .select("id", "longitude", "latitude");

        // join the content of the two tables
        Dataset<Row> result = filtStatCritic.join(stations,
                filtStatCritic.col("station").equalTo(stations.col("id")))
                .select("station", "dayofweek", "hour", "longitude", "latitude", "criticality");

        // sort the result
        Dataset<Row> sortedResult = result.sort(
                new Column("criticality").desc(),
                new Column("station"),
                new Column("dayofweek"),
                new Column("hour"));

        // store the result
        sortedResult.coalesce(1).write().format("csv").option("header", true).save(outputFolder);

        // Close the Spark session
        ss.stop();
    }
}
