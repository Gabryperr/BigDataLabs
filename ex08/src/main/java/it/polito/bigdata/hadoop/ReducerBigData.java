package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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

        double totIncome = 0;

        // Iterate over the set of values and sum them 
        for (DoubleWritable value : values) {
            totIncome += value.get();
        }

        context.write(key, new DoubleWritable(totIncome));
    }
}
