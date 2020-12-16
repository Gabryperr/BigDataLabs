# Remove folders of the previous run
hdfs dfs -rm -r WordCountFilterOut

# Run application
hadoop jar WordCountFilter-1.0.0.jar it.polito.bigdata.hadoop.lab.DriverBigData deb WordCountOut WordCountFilterOut

# Get result
rm WordCountFilterOut.txt
hdfs dfs -getmerge WordCountFilterOut WordCountFilterOut.txt
