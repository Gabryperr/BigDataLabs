package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        NullWritable> {// Output value type

    //create multiple outputs
    MultipleOutputs<Text, NullWritable> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get the temperature from value
        double temp = Double.parseDouble(value.toString().split(",")[3]);

        //emit the entire value
        if (temp <= 30) {
            mos.write("normaltemp", value, NullWritable.get());
        } else {
            mos.write("hightemp", new Text(value.toString().split(",")[3]), NullWritable.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // close the mos
        mos.close();
    }
}
