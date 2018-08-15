package com.charlie.learnmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created on 2018/8/14.
 *
 * @author Xiaolei-Peng
 */
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        for (IntWritable val : values) {
            maxValue = Math.max(maxValue, val.get());
        }
        context.write(key, new IntWritable(maxValue));
    }
}
