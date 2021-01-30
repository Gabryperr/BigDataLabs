package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperAnswersBigData extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text> {// Output value type

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        // get AnswerID, questionID and text from value
        String[] fields = value.toString().split(",", 4);
        String answerID = fields[0];
        String questionId = fields[1];
        String text = fields[3];

        // emit questionID, Answers:answerID,text
        context.write(new Text(questionId), new Text("Answers:" + answerID + "," + text));
    }
}
