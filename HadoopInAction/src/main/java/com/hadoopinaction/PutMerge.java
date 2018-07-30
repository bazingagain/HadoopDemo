package com.hadoopinaction;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * Created on 2018/7/25.
 *
 * @author Xiaolei-Peng
 */
public class PutMerge {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        //面向 HDFS 的FileSystem, FileSystem中包含许多对文件的操作方法,可以多了解下
        FileSystem hdfs  = FileSystem.get(conf);

        //面向 本地文件系统的 的FileSystem
        FileSystem local = FileSystem.getLocal(conf);

        Path inputDir = new Path(args[0]);
        Path hdfsFile = new Path(args[1]);

        try {
            //FileStatus存储目录和文件的元数据, 下面是列出一个目录下的文件
            FileStatus[] inputFiles = local.listStatus(inputDir);
            FSDataOutputStream out = hdfs.create(hdfsFile);

            for (int i=0; i<inputFiles.length; i++) {
                System.out.println(inputFiles[i].getPath().getName());
                FSDataInputStream in = local.open(inputFiles[i].getPath());
                byte buffer[] = new byte[256];
                int bytesRead = 0;
                while( (bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                in.close();
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
