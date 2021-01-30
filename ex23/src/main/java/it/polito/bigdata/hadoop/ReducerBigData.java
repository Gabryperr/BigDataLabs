package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        Text,           // Input key type
        Text,    // Input value type
        Text,           // Output key type
        NullWritable> {  // Output value type

    String user;
    Set<String> friends;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        user = context.getConfiguration().get("user");
        // initialize the set
        friends = new HashSet<>();
    }

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        List<String> localFriends = new ArrayList<>();

        // store all the friends
        for (Text value : values) {
            localFriends.add(value.toString());
        }

        // add the potential friends
        if (localFriends.contains(user)) {
            for (String localFriend : localFriends) {
                if (!localFriend.equals(user)) {
                    friends.add(localFriend);
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        StringBuilder friendList = new StringBuilder();
        for (String friend : friends) {
            friendList.append(friend).append(" ");
        }
        context.write(new Text(friendList.toString()), NullWritable.get());
    }
}
