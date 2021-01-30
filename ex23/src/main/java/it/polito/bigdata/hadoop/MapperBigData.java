package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

    String user;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        user = context.getConfiguration().get("user");
    }

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // split the two users
        String[] users = value.toString().split(",");

        // check if user is present in one of them and emit the other
        if (users[0].equals(user)) {
            // case of user,other -> emit other, user
            context.write(new Text(users[1]), new Text(users[0]));
        } else if (users[1].equals(user)) {
            // case of other,user -> emit other, user
            context.write(new Text(users[0]), new Text(users[1]));
        } else {
            // case of other1, other2 -> emit other1,other2 and other2, other1
            context.write(new Text(users[0]), new Text(users[1]));
            context.write(new Text(users[1]), new Text(users[0]));
        }
    }
}
