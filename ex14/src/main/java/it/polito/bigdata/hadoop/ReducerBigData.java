package it.polito.bigdata.hadoop;

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
        NullWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<NullWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());
    }
}
