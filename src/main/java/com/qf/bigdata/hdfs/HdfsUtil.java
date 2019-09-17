package com.qf.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

public class HdfsUtil {

    public static FileSystem getClient() {
        FileSystem fs = null;

        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        try {
            fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "root");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fs;
    }


    public static void closeClient(FileSystem fs){
        if(fs!=null){
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
