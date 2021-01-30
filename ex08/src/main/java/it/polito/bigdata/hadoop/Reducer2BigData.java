package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer2BigData extends Reducer<
        Text,           // Input key type
        DoubleWritable,    // Input value type
        Text,           // Output key type
        DoubleWritable> { // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<DoubleWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        double totIncome = 0;
        int totMonths = 0;

        // Iterate over the set of values and sum them
        for (DoubleWritable value : values) {
            totIncome += value.get();
            totMonths++;
        }

        // compute the average
        context.write(key, new DoubleWritable(totIncome / (double) totMonths));
    }

}
