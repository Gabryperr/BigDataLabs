# Remove folders of the previous run
#hdfs dfs -rm -r example_data
hdfs dfs -rm -r BiGramCount

# Put input data collection into hdfs
#hdfs dfs -put example_data


# Run application
hadoop jar BiGramCount-1.0.0.jar it.polito.bigdata.hadoop.DriverBigData 10 /data/students/bigdata-01QYD/Lab1/finefoods_text.txt BiGramCount 

# Get result
rm BiGramCount.txt
hdfs dfs -getmerge BiGramCount BiGramCount.txt
sort -k2,2nr BiGramCount.txt | less




