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
        IntWritable> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");

        String SID = fields[0];
        String date = fields[2];

        if (date.startsWith("2017")){
            context.write(new Text(SID), new IntWritable(2017));
        }

        if (date.startsWith("2018")){
            context.write(new Text(SID), new IntWritable(2018));
        }
    }
}
