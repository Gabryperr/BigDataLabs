package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer1BigData extends Reducer<
        Text,           // Input key type
        Text,    // Input value type
        Text,           // Output key type
        Text> {  // Output value type


    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        List<String> friendList = new ArrayList<>();

        // store all the potential friends
        for (Text value : values) {
            friendList.add(value.toString());
        }

        // for each user in the list emit all the other users
        // only if friendList has more than one item
        if (friendList.size() > 1) {
            for (String currentUser : friendList) {
                // initialize the string with potential friends
                StringBuilder potFriends = new StringBuilder();
                for (String friend : friendList) {
                    if(!friend.equals(currentUser)){
                        potFriends.append(friend).append(" ");
                    }
                }
                // emit current user, potential friends
                context.write(new Text(currentUser),new Text(potFriends.toString()));
            }

        }


    }
}
