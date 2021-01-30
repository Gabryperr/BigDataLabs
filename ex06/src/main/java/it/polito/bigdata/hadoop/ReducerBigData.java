package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        Text,           // Input key type
        MaxMinWritable,    // Input value type
        Text,           // Output key type
        MaxMinWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<MaxMinWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        // create the final average
        MaxMinWritable maxMin = new MaxMinWritable(Double.MIN_VALUE, Double.MAX_VALUE);

        // Iterate over the set of values and put them into a list
        for (MaxMinWritable value : values) {
            maxMin = maxMin.aggregate(value);
        }

        // emit the key and the average
        context.write(key, maxMin);
    }
}
