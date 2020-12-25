package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        ProductScoreWritable> {// Output value type

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the map method */

        // Read all the fields
        // fields[0] = reviewID
        // fields[1] = ProductId
        // fields[2] = UserID
        // fields[3] = ProfileName
        // fields[4] = HelpfulnessNumerator
        // fields[5] = HelpfulnessDenominator
        // fields[6] = score
        // other not important fields
        String[] fields = value.toString().split(",");

        String productID, UserID;
        int score;

        try {
            // send all the values
            productID = fields[1];
            UserID = fields[2];
            score = Integer.parseInt(fields[6]);
            context.write(new Text(UserID), new ProductScoreWritable(productID, score));
        }
        catch (NumberFormatException ignored){
            // do nothing if exception
        }

    }
}
