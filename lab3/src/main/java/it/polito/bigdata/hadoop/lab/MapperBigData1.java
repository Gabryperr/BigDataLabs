package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Mapper for the word count part
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        IntWritable> {// Output value type

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the map method */

        // split the line dividing reviewer and products
        String[] products = value.toString().split(",", 2)[1].split(",");

        // sort the vector to emit pair of products in the right order
        Arrays.sort(products);

        // emits the pair of products comma separated
        for (int i = 0; i < products.length; i++) {
            for (int j = i + 1; j < products.length; j++) {
                if (!products[i].equals(products[j])) {
                    context.write(new Text(products[i] + "," + products[j]), new IntWritable(1));
                }
            }

        }
    }
}
