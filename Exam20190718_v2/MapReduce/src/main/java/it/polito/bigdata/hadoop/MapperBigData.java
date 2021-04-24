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
        IntWritable> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");
        String contributor = fields[3];
        String timestamp = fields[1];
        if (timestamp.startsWith("2019")) {
            String date = timestamp.split("_")[0];
            String month = date.split("/")[1];

            if (month.equals("05")) {
                context.write(new Text(contributor), new IntWritable(5));
            }

            if (month.equals("06")) {
                context.write(new Text(contributor), new IntWritable(6));
            }

        }
    }
}
