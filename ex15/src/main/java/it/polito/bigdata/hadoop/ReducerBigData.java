package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        Text,           // Input key type
        NullWritable,    // Input value type
        Text,           // Output key type
        IntWritable> {  // Output value type

    private int counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        //initialize value
        counter = 0;
    }

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<NullWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        counter++;
        context.write(key, new IntWritable(counter));
    }
}
