package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer2BigData extends Reducer<
        Text,           // Input key type
        Text,    // Input value type
        Text,           // Output key type
        Text> {  // Output value type


    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        Set<String> potFriends = new HashSet<>();

        // add all the potential friends without copies
        for (Text value : values) {
            potFriends.add(value.toString());
        }

        // concatenate all the potential friend
        StringBuilder friendList = new StringBuilder();
        for (String friend : potFriends) {
            friendList.append(friend).append(" ");
        }

        // emit the final list of potential friends
        context.write(key, new Text(friendList.toString()));

    }
}