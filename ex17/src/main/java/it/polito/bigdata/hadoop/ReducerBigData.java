package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        Text,           // Input key type
        DoubleWritable,    // Input value type
        Text,           // Output key type
        DoubleWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<DoubleWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        double maxTemp = Double.MIN_VALUE;

        // find max temp
        for (DoubleWritable value : values) {
            maxTemp = Double.max(maxTemp, value.get());
        }

        context.write(key, new DoubleWritable(maxTemp));
    }
}
