# Remove folders of the previous run
hdfs dfs -rm -r BiGramCountFilterOut

# Run application
hadoop jar BiGramCountFilter-1.0.0.jar it.polito.bigdata.hadoop.lab.DriverBigData like BiGramCountOut BiGramCountFilterOut

# Get result
rm BiGramCountFilterOut.txt
hdfs dfs -getmerge BiGramCountFilterOut BiGramCountFilterOut.txt
