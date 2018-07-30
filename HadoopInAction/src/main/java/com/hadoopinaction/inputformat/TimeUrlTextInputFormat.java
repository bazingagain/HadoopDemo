package com.hadoopinaction.inputformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created on 2018/7/29.
 *
 * @author Xiaolei-Peng
 */
public class TimeUrlTextInputFormat extends FileInputFormat<Text, URLWriteable>{

    public RecordReader<Text, URLWriteable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new TImeUrlLineRecordReader(jobConf, (FileSplit) inputSplit);
    }
}
