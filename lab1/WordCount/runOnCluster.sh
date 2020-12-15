# Remove folders of the previous run
#hdfs dfs -rm -r example_data
hdfs dfs -rm -r WordCountOut

# Put input data collection into hdfs
#hdfs dfs -put example_data


# Run application
hadoop jar WordCount-1.0.0.jar it.polito.bigdata.hadoop.DriverBigData 10 /data/students/bigdata-01QYD/Lab1/finefoods_text.txt  WordCountOut

# Get result
rm WordCountOut.txt
hdfs dfs -getmerge WordCountOut WordCountOut.txt
sort -k2,2nr WordCountOut.txt | less




