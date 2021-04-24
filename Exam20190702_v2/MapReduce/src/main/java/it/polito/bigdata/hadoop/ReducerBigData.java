package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
        Text,           // Input key type
        Text,    // Input value type
        Text,           // Output key type
        Text> {  // Output value type

    @Override

    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        String cpu = null;
        boolean unique = true;

        for (Text value : values) {
            if (cpu == null) {
                cpu = value.toString();
            } else if (!cpu.equals(value.toString())) {
                unique = false;
            }
        }

        if(unique){
            context.write(key, new Text(cpu));
        }


    }
}
