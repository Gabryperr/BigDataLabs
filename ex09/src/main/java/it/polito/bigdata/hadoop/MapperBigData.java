package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        IntWritable> {// Output value type

    private Map<String, Integer> wordsMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        wordsMap = new HashMap<>();
    }

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // Split each sentence in words. Use whitespace(s) as delimiter
        // (=a space, a tab, a line break, or a form feed)
        // The split method returns an array of strings
        String[] words = value.toString().split("\\s+");

        // Iterate over the set of words
        for (String word : words) {
            // Transform word case
            String cleanedWord = word.toLowerCase();

            // increment the word number
            wordsMap.put(cleanedWord, wordsMap.getOrDefault(cleanedWord, 0) + 1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException{

        // emit all the words, value pairs
        for (String word: wordsMap.keySet()) {
            context.write(new Text(word), new IntWritable(wordsMap.get(word)));
        }

    }
}
