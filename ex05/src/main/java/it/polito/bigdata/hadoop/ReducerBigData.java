package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        Text,           // Input key type
        AverageWritable,    // Input value type
        Text,           // Output key type
        AverageWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<AverageWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        // create the final average
        AverageWritable average = new AverageWritable(0,0);

        // Iterate over the set of values and put them into a list
        for (AverageWritable value : values) {
            average = average.sum(value);
        }

        // emit the key and the average
        context.write(key, average);
    }
}
