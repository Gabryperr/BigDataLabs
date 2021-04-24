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
        IntWritable,           // Output key type
        NullWritable> {  // Output value type

    int nRobots;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nRobots = 0;
    }

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<NullWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        nRobots++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        context.write(new IntWritable(nRobots), NullWritable.get());
    }
}
