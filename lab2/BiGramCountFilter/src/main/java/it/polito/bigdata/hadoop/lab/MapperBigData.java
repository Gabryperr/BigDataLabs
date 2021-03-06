package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    private String searchWord;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        searchWord = context.getConfiguration().get("searchWord");
    }

    @Override
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // emit only the key part
        String[] words = key.toString().split(" ");
        if (words[0].equals(searchWord) || words[1].equals(searchWord)) {
            context.write(key, value);
        }

        /* Implement the map method */
    }
}
