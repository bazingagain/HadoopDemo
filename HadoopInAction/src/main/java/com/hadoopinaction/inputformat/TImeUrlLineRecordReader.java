package com.hadoopinaction.inputformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 *
 * 处理输入格式如下:
 * 20180717:080808 http://wiki.apache.org/hadoop
 *
 * 在KeyValueLineRecordReader上进行一层包装
 *
 * Created on 2018/7/29.
 *
 * @author Xiaolei-Peng
 */
public class TImeUrlLineRecordReader implements RecordReader<Text, URLWriteable> {

    private KeyValueLineRecordReader lineRecordReader;
    private Text lineKey, lineValue;

    public TImeUrlLineRecordReader(JobConf conf, FileSplit split) throws IOException {
        lineRecordReader = new KeyValueLineRecordReader(conf, split);
        lineKey = lineRecordReader.createKey();
        lineValue = lineRecordReader.createValue();
    }

    public boolean next(Text key, URLWriteable value) throws IOException {
        if (!lineRecordReader.next(lineKey, lineValue)) {
            return false;
        }
        key.set(lineKey);
        value.set(lineValue.toString());
        return true;
    }

    public Text createKey() {
        return new Text("");
    }

    public URLWriteable createValue() {
        return new URLWriteable();
    }

    public long getPos() throws IOException {
        return lineRecordReader.getPos();
    }

    public void close() throws IOException {
        lineRecordReader.close();
    }

    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }
}
