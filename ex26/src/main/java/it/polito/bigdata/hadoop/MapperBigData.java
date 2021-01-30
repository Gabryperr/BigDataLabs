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
import java.util.HashMap;
import java.util.Map;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        NullWritable> {// Output value type

    Map<String,String> dictionary;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // store the dictionary
        String line;
        URI[] urisCachedFiles = context.getCacheFiles();

        BufferedReader file = new BufferedReader(new FileReader(new Path(urisCachedFiles[0].getPath()).getName()));

        dictionary = new HashMap<>();
        while ((line = file.readLine()) != null){
            String key = line.split("\\s+")[1];
            String value = line.split("\\s+")[0];
            dictionary.put(key,value);
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

        StringBuilder convertedLine = new StringBuilder();

        // Iterate over the set of words
        for (String word : words) {
            convertedLine.append(dictionary.get(word)).append(" ");
        }

        // emit the converted Line
        context.write(new Text(convertedLine.toString()), NullWritable.get());
    }
}
