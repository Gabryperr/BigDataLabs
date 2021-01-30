package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {


        String[] fields = value.toString().split(",");
        String username = fields[0];
        String movieID = fields[1];
        String startTime = fields[2];

        // emit movieID, userID
        if (startTime.startsWith("2019"))
            context.write(new Text(movieID), new Text(username));
    }
}
