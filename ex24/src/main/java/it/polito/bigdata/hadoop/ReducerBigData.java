package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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

        StringBuilder friendList = new StringBuilder();
        // concatenate all the friends
        for (Text value : values) {
            friendList.append(value.toString()).append(" ");
        }

        // emit user, list of friends
        context.write(key, new Text(friendList.toString()));

    }


}
