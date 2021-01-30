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

    Map<String, String> rules;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // store the rules
        String line;
        URI[] urisCachedFiles = context.getCacheFiles();

        BufferedReader file = new BufferedReader(new FileReader(new Path(urisCachedFiles[0].getPath()).getName()));

        rules = new HashMap<>();
        while ((line = file.readLine()) != null) {

            String[] fields = line.split("\\s+");

            String gender = fields[0].split("=")[1];
            String year = fields[2].split("=")[1];
            String category = fields[4];

            rules.put(gender + year, category);
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
        String[] fields = value.toString().split(",");

        String gender = fields[3];
        String year = fields[4];

        String valueCategory = value.toString() + "," + rules.getOrDefault(gender + year, "Unknown");

        // emit the converted Line
        context.write(new Text(valueCategory), NullWritable.get());
    }
}
