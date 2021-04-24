package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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

        int numTourism = 0;
        int numMuseum = 0;

        for (Text value : values) {
            numTourism++;

            if (value.toString().equals("museum")) {
                numMuseum++;
            }
        }

        if (numTourism > 2 && numMuseum >= 1){
            context.write(key,NullWritable.get());
        }


    }
}
