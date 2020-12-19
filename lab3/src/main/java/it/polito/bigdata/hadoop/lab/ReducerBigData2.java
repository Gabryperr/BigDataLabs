package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Vector;

/**
 * Reducer for the top key problem
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
        NullWritable,           // Input key type
        WordCountWritable,    // Input value type
        Text,           // Output key type
        IntWritable> {  // Output value type

    @Override
    protected void reduce(
            NullWritable key, // Input key type
            Iterable<WordCountWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the reduce method */

        // initialize the top k vector
        TopKVector<WordCountWritable> topK = new TopKVector<>(100);

        // update top k vector
        for (WordCountWritable value : values) {
            topK.updateWithNewElement(new WordCountWritable(value));
        }

        // emit the final top k values
        Vector<WordCountWritable> localTopK = topK.getLocalTopK();

        for (WordCountWritable top : localTopK) {
            context.write(new Text(top.getWord()), new IntWritable(top.getCount()));
        }


    }
}
