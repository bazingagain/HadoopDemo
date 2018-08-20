package com.hadoopinaction.datajoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

/**
 * Created on 2018/8/2.
 *
 * @author Xiaolei-Peng
 */
public class DataJoinDC extends Configured implements Tool {


    public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        private Hashtable<String, String> joinData = new Hashtable<String, String>();

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            Path[] cacheFiles = new Path[0];
            try {
                cacheFiles = DistributedCache.getLocalCacheFiles(job);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line = null;
                    String[] tokens = null;
                    BufferedReader joinReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    try {
                        while ((line = joinReader.readLine()) != null) {
                            tokens = line.split(",", 2);
                            joinData.put(tokens[0], tokens[1]);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        joinReader.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void map(Text key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String joinValue = joinData.get(key);
            if (joinValue != null) {
                outputCollector.collect(key, new Text(value.toString() + "," + joinValue));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobConf job = new JobConf(conf, DataJoinDC.class);

        //方式1: hadoop jar datajoindc.jar small.txt big.txt output
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
        Path in = new Path(args[1]);
        Path out = new Path(args[2]);



        /**
         * 方式2: small.txt在用户的本地中,不在hdfs中, 则可以使用该种方式, 将small.txt上传到各节点, 这种方式不用写DistributedCache
         //hadoop jar datajoindc.jar small.txt big.txt output
         Path in = new Path(args[0]);
         Path out = new Path(args[1]);
         */

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("DataJoin with DistributeCache");
        job.setMapperClass(MapClass.class);
        job.setNumReduceTasks(0);
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.set("key.value.seperator.in.input.line", ",");
        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new DataJoinDC(),
                args);

        System.exit(res);
    }
}
