# Remove folders of the previous run
hdfs dfs -rm -r SparkFilterOut

# Run application
spark-submit --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master yarn SparkFilter-1.0.0.jar /data/students/bigdata-01QYD/Lab2/ SparkFilterOut ho

# Get result
rm SparkFilterOut.txt
hdfs dfs -getmerge SparkFilterOut SparkFilterOut.txt