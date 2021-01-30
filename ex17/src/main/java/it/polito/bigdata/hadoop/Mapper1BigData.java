package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Basic MapReduce Project - Mapper
 */
class Mapper1BigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        DoubleWritable> {// Output value type

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get date from value
        String date = value.toString().split(",")[1];

        // get temp from value
        double temp = Double.parseDouble(value.toString().split(",")[3]);

        // emit date, temp
        context.write(new Text(date), new DoubleWritable(temp));

    }
}
