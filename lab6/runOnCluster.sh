# Remove folders of the previous run
hdfs dfs -rm -r SparkPeopleAlsoLike

# Run application
spark-submit --class it.polito.bigdata.spark.example.SparkDriver --deploy-mode client --master yarn SparkPeopleAlsoLike-1.0.0.jar /data/students/bigdata-01QYD/Lab4/Reviews.csv SparkPeopleAlsoLike

# Get result
rm SparkPeopleAlsoLike.txt
hdfs dfs -getmerge SparkPeopleAlsoLike SparkPeopleAlsoLike.txt