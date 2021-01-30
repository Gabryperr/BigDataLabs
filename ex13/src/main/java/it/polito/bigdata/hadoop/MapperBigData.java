package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        NullWritable,         // Output key type
        DateIncomeWritable> {// Output value type

    // define the top
    DateIncomeWritable top;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //initialize the top
        top = new DateIncomeWritable("", Integer.MIN_VALUE);
    }

    @Override
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get date from key
        String date = key.toString();

        // get income from value
        int income = Integer.parseInt(value.toString());

        DateIncomeWritable currentIncome = new DateIncomeWritable(date, income);

        //  update the top
        if (currentIncome.compareTo(top) > 0) {
            top = new DateIncomeWritable(currentIncome);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // emit the local top
        context.write(NullWritable.get(), top);
    }

}
