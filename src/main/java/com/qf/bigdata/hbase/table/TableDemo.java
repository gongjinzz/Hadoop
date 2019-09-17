package com.qf.bigdata.hbase.table;

import com.qf.bigdata.hbase.util.HbaseTools;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TableDemo {

    private static Admin admin=null;
    private final TableName TABLENAME=TableName.valueOf("ns2:demo2");

    @Before
    public void init(){
        admin= HbaseTools.getAdmin();
    }
    @After
    public void closeAdmin(){
        HbaseTools.closeAdmin(admin);
    }

    /**
     * 创建一个表
     */
    @Test
    public void createTable() throws IOException {

        //获取一个表的描述对象
        HTableDescriptor ht=new HTableDescriptor(TABLENAME);
        //创建一个列族描述器
        HColumnDescriptor hc=new HColumnDescriptor(Bytes.toBytes("base_info"));
        hc.setVersions(1,5);
        hc.setTimeToLive(24*60*60);
        HColumnDescriptor hc1=new HColumnDescriptor(Bytes.toBytes("extra_info"));
        hc1.setVersions(3,5);
        hc.setTimeToLive(24*60*60);
        //将列族加入到表的描述对象中
        ht.addFamily(hc).addFamily(hc1);
        //提交创建请求
        admin.createTable(ht);


    }
}
