package com.qf.bigdata.hbase.data;

import com.qf.bigdata.hbase.util.HbaseTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class PutDemo {

    public static void main(String[] args) throws IOException {
       Configuration conf=new Configuration();
       conf.set("hbase.zookeeper.quorum","mini1:2181,mini2:2181,mini3:2181");
       HTable ht=new HTable(conf,TableName.valueOf("ns1:demo"));
       Put put=new Put(Bytes.toBytes("rk0003"));
       put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("wangwu"));
       put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes("26"));
       put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("sex"),Bytes.toBytes("0"));
       ht.put(put);
        System.out.println("添加成功");

    }

    @Test
    public void one() {

        Admin admin = HbaseTools.getAdmin();
        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf("ns1:demo"));

        HColumnDescriptor hc = new HColumnDescriptor(Bytes.toBytes("base_info"));
        ht.addFamily(hc);
        HColumnDescriptor hc1 = new HColumnDescriptor(Bytes.toBytes("extra_info"));
        ht.addFamily(hc1);

        try {
            admin.createTable(ht);
            System.out.println("创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
