# Remove folders of the previous run
hdfs dfs -rm -r CouplesOccurrences
hdfs dfs -rm -r Top100

# Run application
hadoop jar PeopleAlsoLike-1.0.0.jar it.polito.bigdata.hadoop.lab.DriverBigData 4 /data/students/bigdata-01QYD/Lab3/AmazonTransposedDataset_Sample.txt CouplesOccurrences Top100

# Get result
rm CouplesOccurrences.txt
hdfs dfs -getmerge CouplesOccurrences CouplesOccurrences.txt

rm Top100.txt
hdfs dfs -getmerge Top100 Top100.txt