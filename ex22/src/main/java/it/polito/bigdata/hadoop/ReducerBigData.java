package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        NullWritable,           // Input key type
        Text,    // Input value type
        Text,           // Output key type
        NullWritable> {  // Output value type

    @Override
    protected void reduce(
            NullWritable key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        // concatenate the list of friends
        StringBuilder friends = new StringBuilder();
        for (Text value : values) {
            friends.append(value).append(" ");
        }

        // emit the list of friends
        context.write(new Text(friends.toString()), NullWritable.get());
    }
}
