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

        // create separate lists for questions and answers
        List<String> questions = new ArrayList<>();
        List<String> answers = new ArrayList<>();

        // store questions and answers
        for (Text value : values) {
            String[] fields = value.toString().split(":", 2);
            String tag = fields[0];
            String data = fields[1];

            if (tag.equals("Questions")) {
                questions.add(data);
            }

            if (tag.equals("Answers")) {
                answers.add(data);
            }
        }

        // iterate over questions and answers and emit all the combination
        for (String question : questions) {
            for (String answer : answers) {
                context.write(new Text(key.toString() + "," + question + "," + answer), NullWritable.get());

            }
        }
    }
}
