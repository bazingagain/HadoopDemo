package com.hadoopinaction.mapredprimitive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 * 对专利引用文件的处理
 *[引用, 被引]
 * 10078,3923798
 * 10078,3923732
 * 10078,3921212
 *
 *
 * Created on 2018/7/29.
 *
 * @author Xiaolei-Peng
 */

public class MyJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        //hadoop jar playground/MyJob.jar jar MyJob -D mapred.reduce.task=0 input/cite75_99.txt about
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);

        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobConf job = new JobConf(conf, MyJob.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("MyJob");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);      //设置K2的类型, 即mapper的输出,reduce的输入key的类型
        job.setOutputValueClass(Text.class);    //设置V2的类型, 即mapper的输出,reduce的输入value的类型
        job.set("key.value.separator.in.input.line", ",");

        JobClient.runJob(job);

        return 0;
    }

    public static class MapClass extends MapReduceBase
            implements Mapper<Text, Text, Text, Text> {
        /*
            要注意的是:key,value的类型要与run()方法中JobConf设置的KeyValueTextInputFormat.class的k,v类型一致
         */
        public void map(Text key, Text value,
                        OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {
            /*
             * 由于输入的数据格式为[专利号, 被引用专利号], 而我们是计算专利的被引用次数,所以以value为k2
             */
            output.collect(value, key);
        }
    }

    public static class Reduce extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {
        /*
           要注意的是:key,values的类型要与MapperClass中map的OutputCollector<K,V>中K,V的类型一致
           reduce的OutputCollector<K,V>中K,V类型要与run()方法中JobConf设置的TextOutputFormat.class的k,v类型一致
         */
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {

            String csv = "";
            while (values.hasNext()) {
                if (csv.length() > 0) csv += ",";
                csv += values.next().toString();
            }
            output.collect(key, new Text(csv));
        }
    }

    public static class CountReduce extends MapReduceBase
            implements Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {

            int count = 0;
            while (values.hasNext()) {
                values.next();
                count++;
            }
            output.collect(key, new IntWritable(count));
        }
    }

}
