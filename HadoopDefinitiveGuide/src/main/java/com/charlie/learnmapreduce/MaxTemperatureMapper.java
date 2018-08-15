package com.charlie.learnmapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created on 2018/8/14.
 *
 * @author Xiaolei-Peng
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    }
}
