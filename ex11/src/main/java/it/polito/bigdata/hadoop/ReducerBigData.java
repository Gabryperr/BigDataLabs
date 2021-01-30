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

        double sum = 0;
        int occurrences = 0;

        // Iterate over the set of values and sum them 
        for (DoubleWritable value : values) {
            occurrences++;
            sum += value.get();
        }

        // emit the average
        context.write(key, new DoubleWritable(sum / (double) occurrences));
    }
}
