package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
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
        DoubleWritable> {// Output value type

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get sensorID from value
        String sensorID = value.toString().split(",")[0];

        // get PM10 from value
        double pm10 = Double.parseDouble(value.toString().split(",")[2]);

        // emit sensorID,pm10
        context.write(new Text(sensorID), new DoubleWritable(pm10));
    }
}
