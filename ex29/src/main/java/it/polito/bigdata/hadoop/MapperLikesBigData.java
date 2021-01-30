package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperLikesBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get userId, movieGenre
        String[] fields = value.toString().split(",");
        String userId = fields[0];
        String movieGenre = fields[1];

        if (movieGenre.equals("Commedia") || movieGenre.equals("Adventure")) {
            context.write(new Text(userId), new Text("Likes:" + movieGenre));
        }
    }
}
