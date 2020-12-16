package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        NullWritable> {// Output value type

    private String searchWord;

    @Override
    protected void setup(Context context) {
        searchWord = context.getConfiguration().get("searchWord");
    }

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // emit only the key part
        String[] words = value.toString().split("\\s+");
        if (words[0].equals(searchWord) || words[1].equals(searchWord)) {
            context.write(value, NullWritable.get());
        }

        /* Implement the map method */
    }
}
