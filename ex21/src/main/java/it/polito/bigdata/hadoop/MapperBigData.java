package it.polito.bigdata.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        NullWritable> {// Output value type

    List<String> stopwords;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        stopwords = new ArrayList<>();

        // Retrieve the original path of distributed file
        URI[] urisCachedFiles = context.getCacheFiles();

        // read the file
        BufferedReader file = new BufferedReader(new FileReader(new Path(urisCachedFiles[0].getPath()).getName()));


        // iterate over the file
        String line;
        while ((line = file.readLine()) != null) {
            stopwords.add(line);
        }
        file.close();

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

        StringBuilder sentence = new StringBuilder();

        // Iterate over the set of words
        for (String word : words) {
            if (!stopwords.contains(word)){
                sentence.append(word).append(" ");
            }
        }

        // emit the sentence
        context.write(new Text(sentence.toString()), NullWritable.get());
    }
}
