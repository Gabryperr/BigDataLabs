package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Vector;

/**
 * Mapper for the top key problem
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
        Text,           // Input key type
        Text,           // Input value type
        NullWritable,           // Output key type
        WordCountWritable> {  // Output value type

    private TopKVector<WordCountWritable> topK;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // initialize the top k vector
        topK = new TopKVector<>(100);
    }

    @Override
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the map method */

        // retrieve the content from key and value
        String products = key.toString();
        Integer occurrences = Integer.parseInt(value.toString());

        // update top k vector
        topK.updateWithNewElement(new WordCountWritable(products, occurrences));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //emit the local top k
        Vector<WordCountWritable> localTopK = topK.getLocalTopK();

        for (WordCountWritable top : localTopK) {
            context.write(NullWritable.get(), top);
        }
    }
}
