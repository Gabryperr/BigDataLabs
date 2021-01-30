package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // split the two users
        String[] users = value.toString().split(",");


        // emit user1,user2 and user2,user1
        context.write(new Text(users[0]), new Text(users[1]));
        context.write(new Text(users[1]), new Text(users[0]));

    }
}
