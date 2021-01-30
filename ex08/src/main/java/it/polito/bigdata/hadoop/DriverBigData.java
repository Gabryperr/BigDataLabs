package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce program
 */
public class DriverBigData extends Configured
        implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath;
        Path outputDir;
        int numberOfReducers;
        int exitCode;

        // Parse the parameters
        // Number of instances of the reducer class
        numberOfReducers = Integer.parseInt(args[0]);
        // Folder containing the input data
        inputPath = new Path(args[1]);
        // Output folder
        outputDir = new Path(args[2]);

        Configuration conf = this.getConf();

        // Define a new job
        Job job = Job.getInstance(conf);

        // Assign a name to the job
        job.setJobName("ex08");

        // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
        FileInputFormat.addInputPath(job, inputPath);

        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputDir);

        // Specify the class of the Driver for this job
        job.setJarByClass(DriverBigData.class);

        // Set job input format
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set map class
        job.setMapperClass(MapperBigData.class);

        // Set map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set reduce class
        job.setReducerClass(ReducerBigData.class);

        // Set reduce output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set number of reducers
        job.setNumReduceTasks(numberOfReducers);


        // Execute the job and wait for completion
        if (job.waitForCompletion(true)) {

            Path outputDir2;

            // Output folder2
            outputDir2 = new Path(args[3]);

            // Define a new job
            Job job2 = Job.getInstance(conf);

            // Assign a name to the job
            job2.setJobName("ex08 part2");

            // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
            FileInputFormat.addInputPath(job2, outputDir);

            // Set path of the output folder for this job
            FileOutputFormat.setOutputPath(job2, outputDir2);

            // Specify the class of the Driver for this job
            job2.setJarByClass(DriverBigData.class);

            // Set job input format
            job2.setInputFormatClass(KeyValueTextInputFormat.class);

            // Set job output format
            job2.setOutputFormatClass(TextOutputFormat.class);

            // Set map class
            job2.setMapperClass(Mapper2BigData.class);

            // Set map output key and value classes
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(DoubleWritable.class);

            // Set reduce class
            job2.setReducerClass(Reducer2BigData.class);

            // Set reduce output key and value classes
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            // Set number of reducers
            job2.setNumReduceTasks(numberOfReducers);

            // Execute the job and wait for completion
            if (job2.waitForCompletion(true))
                exitCode = 0;
            else
                exitCode = 2;
        } else
            exitCode = 1;

        return exitCode;
    }


    /**
     * Main of the driver
     */

    public static void main(String[] args) throws Exception {
        // Exploit the ToolRunner class to "configure" and run the Hadoop application
        int res = ToolRunner.run(new Configuration(),
                new DriverBigData(), args);

        System.exit(res);
    }
}