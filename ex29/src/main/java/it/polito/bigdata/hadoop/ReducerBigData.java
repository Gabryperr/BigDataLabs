package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        Text,           // Input key type
        Text,    // Input value type
        Text,           // Output key type
        NullWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        String genderYear = "";
        List<String> movieGenre = new ArrayList<>();

        // store genderYear and movieGenre
        for (Text value : values) {
            String[] fields = value.toString().split(":");

            String tag = fields[0];
            if(tag.equals("Users")){
                genderYear = fields[1];
            }
            if(tag.equals("Likes")){
                movieGenre.add(fields[1]);
            }
        }

        // emit genderYear if movieGenre.size = 2
        if(movieGenre.size() == 2){
            context.write(new Text(genderYear), NullWritable.get());
        }
    }
}
