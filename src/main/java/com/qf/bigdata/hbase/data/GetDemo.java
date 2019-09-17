package com.qf.bigdata.hbase.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.groovy.runtime.powerassert.SourceText;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class GetDemo {
    private static Table table = null;
    Connection conn = null;

    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "mini1:2181,mini2:2181,mini3:2181");
        conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf("ns1:demo"));
    }

    @After
    public void close() {

        try {
            table.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getData() throws IOException {
        //获取数据
        Get get = new Get(Bytes.toBytes("rk0001"));
        Result result = table.get(get);
        CellScanner cellScanner = result.cellScanner();
//        Cell current = cellScanner.current();
//        System.out.println(current.getRow());
        System.out.println(Bytes.toString(result.getRow()));
        while(cellScanner.advance()){
            Cell current=cellScanner.current();
            System.out.print(new String(CellUtil.cloneFamily(current),"utf-8"));
            System.out.print(":"+new String(CellUtil.cloneQualifier(current),"utf-8"));
            System.out.println("\t"+new String(CellUtil.cloneValue(current),"utf-8"));
        }

    }
}
