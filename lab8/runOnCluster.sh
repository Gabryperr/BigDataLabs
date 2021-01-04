# Remove folders of the previous run
hdfs dfs -rm -r SparkBikeSQL

# Run application
spark-submit --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master yarn SparkBikeSQL-1.0.0.jar /data/students/bigdata-01QYD/Lab7/register.csv /data/students/bigdata-01QYD/Lab7/stations.csv 0.6 SparkBikeSQL

# Get result
rm SparkBikeSQL.txt
hdfs dfs -getmerge SparkBikeSQL SparkBikeSQL.txt