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

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get customerID and bookID
        String[] fields = value.toString().split(",");
        String customerID = fields[0];
        String bookId = fields[1];
        String date = fields[2];

        // emit ((customerID,bookID), 1) if year is 2018
        if (date.startsWith("2018")) {
            context.write(new Text(customerID + "," + bookId), new IntWritable(1));
        }

    }
}
