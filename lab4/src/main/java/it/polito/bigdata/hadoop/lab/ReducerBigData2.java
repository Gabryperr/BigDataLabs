package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
        Text,           // Input key type
        DoubleWritable,    // Input value type
        Text,           // Output key type
        DoubleWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<DoubleWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the reduce method */

        // compute the mean for each key
        double sum = 0;
        int count = 0;

        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }

        // emit the final mean
        context.write(key, new DoubleWritable(sum / count));
    }
}
