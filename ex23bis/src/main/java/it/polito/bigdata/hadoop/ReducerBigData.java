package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
    List<String> friends;
    Set<String> potFriends;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        user = context.getConfiguration().get("user");
        // initialize the set
        potFriends = new HashSet<>();
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

        // if key is user the list is the list of friends
        if (key.toString().equals(user)){
            friends = new ArrayList<>(localFriends);
        }

        // add the potential friends
        if (localFriends.contains(user)) {
            for (String localFriend : localFriends) {
                if (!localFriend.equals(user)) {
                    potFriends.add(localFriend);
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // remove friends from potential friends
        potFriends.removeAll(friends);

        StringBuilder friendList = new StringBuilder();
        for (String friend : potFriends) {
            friendList.append(friend).append(" ");
        }
        context.write(new Text(friendList.toString()), NullWritable.get());
    }
}
