package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Vector;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
        Text,           // Input key type
        ProductScoreWritable,    // Input value type
        Text,           // Output key type
        DoubleWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<ProductScoreWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        Vector<ProductScoreWritable> products = new Vector<>();
        int totalScore = 0;

        // save all the products related to a user and compute the total score
        for (ProductScoreWritable value : values) {
            products.add(new ProductScoreWritable(value));
            totalScore += value.getScore();
        }

        //compute the mean value
        double mean = (double) totalScore / (double) products.size();

        // send data
        for (ProductScoreWritable product : products) {
            context.write(new Text(product.getProductID()), new DoubleWritable(product.getScore() - mean));

        }


        /* Implement the reduce method */

    }
}
