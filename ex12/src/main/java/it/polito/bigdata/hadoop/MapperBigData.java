package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    private double threshold;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
    }

    @Override
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get PM10 from value
        double pm10 = Double.parseDouble(value.toString());

        // if PM10 is below threshold emit the entire line
        if (pm10 < threshold) {
            context.write(key, value);
        }
    }
}
