package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import it.polito.bigdata.hadoop.DriverBigData.COUNTERS;

import java.io.IOException;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        NullWritable,         // Output key type
        NullWritable> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // increment counter
        context.getCounter(COUNTERS.INPUT_LINES).increment(1);

    }

}
