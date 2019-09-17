package com.qf.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


import java.net.URI;




public class HdfsDemo {

    private static  Logger logger= Logger.getLogger(HdfsDemo.class);
    public static void main(String[] args) {

        Configuration conf=new Configuration();
        conf.addResource("hdfs-site.xml");
        try {
            FileSystem fs=FileSystem.get(new URI("hdfs://mini:9000"),conf,"root");
        } catch (Exception e) {
            System.out.println("产生了日志");
        }
    }

}
