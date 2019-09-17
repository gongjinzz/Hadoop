package com.qf.bigdata.hbase.namespace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class CreateNamespace {

    public static void main(String[] args) throws IOException {

        Configuration conf=new Configuration();
        conf.set("hbase.zookeeper.quorum","mini1:2181,mini2:2181,mini3:2181");
        HBaseAdmin admin=new HBaseAdmin(conf);
        NamespaceDescriptor namespaceDescriptor= NamespaceDescriptor.create("ns3").build();
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        System.out.println("创建成功");

    }

}
