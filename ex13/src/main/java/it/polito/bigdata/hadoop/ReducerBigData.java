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
        NullWritable,           // Input key type
        DateIncomeWritable,    // Input value type
        Text,           // Output key type
        IntWritable> {  // Output value type

    @Override

    protected void reduce(
            NullWritable key, // Input key type
            Iterable<DateIncomeWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        DateIncomeWritable top = new DateIncomeWritable("", Integer.MIN_VALUE);

        // Iterate over the set of values and find the top
        for (DateIncomeWritable value : values) {
            if (value.compareTo(top) > 0) {
                top = new DateIncomeWritable(value);
            }
        }
        // emit the final value
        context.write(new Text(top.getDate()), new IntWritable(top.getIncome()));
    }
}
