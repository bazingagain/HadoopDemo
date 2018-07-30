package com.hadoopinaction.inputformat;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created on 2018/7/29.
 *
 * @author Xiaolei-Peng
 */
public class URLWriteable implements Writable {
    protected URL url;

    public URLWriteable() {
    }

    public URLWriteable(URL url) {
        this.url = url;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(url.toString());
    }

    public void readFields(DataInput dataInput) throws IOException {
        url = new URL(dataInput.readUTF());
    }

    public void set(String url) throws MalformedURLException {
        this.url = new URL(url);
    }
}
