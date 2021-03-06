package com.hadoopinaction.mulitipleoutput;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 *
 * 将输入数据的不同列提取为不同文件的程序
 *
 * Created on 2018/8/12.
 *
 * @author Xiaolei-Peng
 */
public class MultiFile extends Configured implements Tool {
    public static class MapClass extends MapReduceBase
            implements Mapper<LongWritable, Text, NullWritable, Text> {

        private MultipleOutputs mos;
        private OutputCollector<NullWritable, Text> collector;

        public void configure(JobConf conf) {
            mos = new MultipleOutputs(conf);
        }

        public void map(LongWritable key, Text value,
                        OutputCollector<NullWritable, Text> output,
                        Reporter reporter) throws IOException {

            String[] arr = value.toString().split(",", -1);
            String chrono = arr[0] + "," + arr[1] + "," + arr[2];
            String geo    = arr[0] + "," + arr[4] + "," + arr[5];

            collector = mos.getCollector("chrono", reporter);
            collector.collect(NullWritable.get(), new Text(chrono));
            collector = mos.getCollector("geo", reporter);
            collector.collect(NullWritable.get(), new Text(geo));
        }

        public void close() throws IOException {
            mos.close();
        }
    }

    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        // Create a JobConf using the processed conf
        JobConf job = new JobConf(conf, MultiFile.class);

        // Process custom command-line options
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // Specify various job-specific parameters
        job.setJobName("MultiFile");
        job.setMapperClass(MapClass.class);

        job.setInputFormat(TextInputFormat.class);
//        job.setOutputFormat(PartitionByCountryMTOF.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);


        MultipleOutputs.addNamedOutput(job,
                "chrono",
                TextOutputFormat.class,
                NullWritable.class,
                Text.class);
        MultipleOutputs.addNamedOutput(job,
                "geo",
                TextOutputFormat.class,
                NullWritable.class,
                Text.class);

        // Submit the job, then poll for progress until the job is complete
        JobClient.runJob(job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new Configuration(), new MultiFile(), args);

        System.exit(res);
    }
}
