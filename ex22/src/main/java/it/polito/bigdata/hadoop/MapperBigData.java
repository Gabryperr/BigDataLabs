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
        NullWritable,         // Output key type
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
        if (users[0].equals(user)){
            context.write(NullWritable.get(), new Text(users[1]));
        }
        if (users[1].equals(user)){
            context.write(NullWritable.get(), new Text(users[0]));
        }
    }
}
