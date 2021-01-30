package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;
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
        DoubleWritable> {// Output value type

    @Override
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get year and month from key
        String year = key.toString().split("-")[0];
        String month = key.toString().split("-")[1];

        // get income from value
        double income = Double.parseDouble(value.toString());

        // emit year-month, income
        context.write(new Text(year + "-" + month), new DoubleWritable(income));
    }
}
