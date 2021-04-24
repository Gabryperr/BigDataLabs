package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");

        String city = fields[3];
        String country = fields[4];
        String category = fields[5];
        String subcategory = fields[6];

        if (country.equals("Italy")) {
            if (subcategory.equals("museum")) {
                context.write(new Text(city), new Text(subcategory));

            } else if (category.equals("tourism")) {
                context.write(new Text(city), new Text(category));
            }
        }

    }
}
