# Remove folders of the previous run
hdfs dfs -rm -r SparkBike

# Run application
spark-submit --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master yarn SparkBike-1.0.0.jar /data/students/bigdata-01QYD/Lab7/register.csv /data/students/bigdata-01QYD/Lab7/stations.csv 0.6 SparkBike

# Get result
rm SparkBike.txt
hdfs dfs -getmerge SparkBike SparkBike.txt