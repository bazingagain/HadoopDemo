package com.charlie.learnmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created on 2018/8/14.
 *
 * @author Xiaolei-Peng
 */
public class MaxTemperature {

    /**
     * export HADOOP_CLASS = charlielearnhadoop.jar
     * Max Temperature input/sample.txt output
     * hadoop
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max Temperature");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //waitForCompletion submit the job and waits for it to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
