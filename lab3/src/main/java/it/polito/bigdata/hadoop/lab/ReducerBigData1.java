package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer for the word count part
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
        Text,           // Input key type
        IntWritable,    // Input value type
        Text,           // Output key type
        IntWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the reduce method */

        // Count the occurrences
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        // emit the key and the occurrences
        context.write(key, new IntWritable(count));

    }
}
