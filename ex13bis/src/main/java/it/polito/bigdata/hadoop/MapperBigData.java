package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
        Text, // Input key type
        Text,         // Input value type
        NullWritable,         // Output key type
        DateIncomeWritable> {// Output value type

    // define the top array
    DateIncomeWritable[] top;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //initialize the top array
        top = new DateIncomeWritable[2];

        for (int i = 0; i < top.length; i++) {
            top[i] = new DateIncomeWritable("", Integer.MIN_VALUE);
        }
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
        if (currentIncome.compareTo(top[1]) < 0) {
            top[1] = new DateIncomeWritable(currentIncome);
            Arrays.sort(top);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // emit the local top vector

        for (DateIncomeWritable dateIncomeWritable : top) {
            context.write(NullWritable.get(), dateIncomeWritable);
        }


    }

}
