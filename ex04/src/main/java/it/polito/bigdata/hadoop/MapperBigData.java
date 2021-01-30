package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    // PM10 threshold value
    double threshold;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
    }

    @Override
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get zoneID from key
        String zoneID = key.toString().split(",")[0];

        // get date from key
        String date = key.toString().split(",")[1];

        //get PM10 from value
        float pm10 = Float.parseFloat(value.toString());

        // emit zoneID, date if PM10 is above threshold
        if (pm10 > threshold) {
            context.write(new Text(zoneID), new Text(date));
        }
    }
}
