package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        DoubleWritable> {// Output value type

    @Override
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the map method */

        // retrieve data
        String productID = key.toString();
        double score = Double.parseDouble(value.toString());

        // emit data
        context.write(new Text(productID), new DoubleWritable(score));

    }
}
