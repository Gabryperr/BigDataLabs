package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        NullWritable> {// Output value type

    private String begin;

    @Override
    protected void setup(Context context) {
        begin = context.getConfiguration().get("begin");
    }

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // emit only the key part
        String line = value.toString();
        if (line.startsWith(begin)) {
            context.write(value, NullWritable.get());
        }

        /* Implement the map method */
    }
}
