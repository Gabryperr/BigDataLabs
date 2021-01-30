package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.avro.JsonProperties;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        Text,           // Input key type
        Text,    // Input value type
        Text,           // Output key type
        NullWritable> {  // Output value type

    @Override

    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        // use a boolean to check if the user is unique
        String username = null;
        boolean unique = true;

        for (Text value : values) {
            if (username == null)
                username = value.toString();
            else if (!username.equals(value.toString()))
                unique = false;
        }

        if (unique)
            context.write(key, NullWritable.get());
    }
}
