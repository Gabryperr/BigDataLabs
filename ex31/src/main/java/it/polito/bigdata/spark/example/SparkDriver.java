package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("ex31");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file/folder
		JavaRDD<String> linesRDD = sc.textFile(inputPath);

		// Filter the lines
		JavaRDD<String> filteredRDD = linesRDD.filter(line-> line.contains("google"));

		// get distinct IP
		JavaRDD<String> distinctIpRDD = filteredRDD.map(line -> line.split("\\s+")[0]).distinct();

		// Store the output
		distinctIpRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
