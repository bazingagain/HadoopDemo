package com.hadoopinaction.practice;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Created on 2018/7/26.
 *
 * @author Xiaolei-Peng
 */
public class EdgePartitioner implements Partitioner<Edge, Writable> {
    /*
        返回键/值对要发送到哪个reducer
     */
    public int getPartition(Edge key, Writable writable, int numPartition) {
        return key.getDepartureNode().hashCode() % numPartition;
    }

    public void configure(JobConf jobConf) {

    }
}
