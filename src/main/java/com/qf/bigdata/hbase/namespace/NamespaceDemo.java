package com.qf.bigdata.hbase.namespace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class NamespaceDemo {

    public static void main(String[] args) throws IOException {

        Configuration conf=new Configuration();
        conf.set("hbase.zookeeper.quorum","mini1:2181,mini2:2181,mini3:2181");

        //获取一个连接
        Connection conn=ConnectionFactory.createConnection(conf);
        Admin admin=conn.getAdmin();
        NamespaceDescriptor namespaceDescriptor=NamespaceDescriptor.create("connectionfactory").build();
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        System.out.println("成功创建namespace");
    }
}
