package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperUsersBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get userID, gender, year
        String[] fields = value.toString().split(",");
        String userID = fields[0];
        String gender = fields[3];
        String year = fields[4];

        // emit userId, gender + year
        context.write(new Text(userID), new Text("Users:" + gender + "," + year));
    }
}
