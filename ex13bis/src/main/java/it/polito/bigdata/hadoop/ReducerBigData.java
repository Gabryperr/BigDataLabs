package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.Arrays;

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

        //initialize the top array
        DateIncomeWritable[] top = new DateIncomeWritable[2];

        for (int i = 0; i < top.length; i++) {
            top[i] = new DateIncomeWritable("", Integer.MIN_VALUE);
        }

        // Iterate over the set of values and find the top
        for (DateIncomeWritable value : values) {
            if (value.compareTo(top[1]) < 0) {
                top[1] = new DateIncomeWritable(value);
                Arrays.sort(top);
            }
        }
        // emit the final value
        for (DateIncomeWritable dateIncomeWritable : top) {
            context.write(new Text(dateIncomeWritable.getDate()), new IntWritable(dateIncomeWritable.getIncome()));
        }


    }
}
