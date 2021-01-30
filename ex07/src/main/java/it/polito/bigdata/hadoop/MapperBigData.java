package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    @Override
    protected void map(
            Text key,   // Input key type
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

            // check if word is and, or, not
            if (!cleanedWord.equals("and") && !cleanedWord.equals("or") && !cleanedWord.equals("not")) {

                // emit the pair (word, key)
                context.write(new Text(cleanedWord), key);
            }
        }
    }
}
