package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Basic MapReduce Project - Mapper
 */
class Mapper2BigData extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    @Override
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // split all the friends
        String[] friends = value.toString().split("\\s+");

        for (String friend: friends) {
            // emit all user, potential friends
            context.write(key, new Text(friend));
        }
    }
}