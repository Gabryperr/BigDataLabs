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
        Text,    // Input value type
        Text,           // Output key type
        Text> {  // Output value type

    @Override

    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        List<String> dates = new ArrayList<>();

        // Iterate over the set of values and put them into a list
        for (Text value : values) {
            dates.add(value.toString());
        }

        // emit the key and the list of dates
        context.write(key, new Text(dates.toString()));
    }
}
