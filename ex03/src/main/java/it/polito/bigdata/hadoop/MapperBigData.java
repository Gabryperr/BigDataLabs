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
        IntWritable> {// Output value type

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

        // get sensorID from key
        String sensorID = key.toString().split(",")[0];

        //get PM10 from value
        float pm10 = Float.parseFloat(value.toString());

        // emit sensorID,+1 if PM10 is above threshold
        if (pm10 > threshold) {
            context.write(new Text(sensorID), new IntWritable(1));
        }
    }
}
