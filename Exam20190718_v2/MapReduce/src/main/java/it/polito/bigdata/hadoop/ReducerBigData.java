package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        Text,           // Input key type
        IntWritable,    // Input value type
        Text,           // Output key type
        NullWritable> {  // Output value type

    @Override

    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        int mayCommits = 0;
        int juneCommits = 0;

        for (IntWritable value : values) {
            if (value.get() == 5) {
                mayCommits++;
            } else {
                juneCommits++;
            }
        }

        if (juneCommits > mayCommits) {
            context.write(key, NullWritable.get());
        }

    }
}
