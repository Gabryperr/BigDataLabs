package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

        Set<String> sentences = new HashSet<>();

        // Iterate over the set of values and sum them 
        for (Text value : values) {
            sentences.add(value.toString());
        }

        context.write(key, new Text(sentences.toString()));
    }
}
