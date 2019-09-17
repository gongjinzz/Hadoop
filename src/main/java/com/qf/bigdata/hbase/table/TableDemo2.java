package com.qf.bigdata.hbase.table;

import com.qf.bigdata.hbase.util.HbaseTools;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TableDemo2 {

    private static Admin admin = null;
    private static final TableName TABLENAME = TableName.valueOf("ns2:demo2");

    @Before
    public void init() {
        admin = HbaseTools.getAdmin();
    }

    @After
    public void closeAdmin() {
        HbaseTools.closeAdmin(admin);
    }

    @Test
    public void createTable() {
        HTableDescriptor ht = new HTableDescriptor(TABLENAME);
        HColumnDescriptor hc = new HColumnDescriptor(Bytes.toBytes("base_info"));
        ht.addFamily(hc);
        try {
            admin.createTable(ht);
            System.out.println("创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteTable() {

        try {
            if (admin.tableExists(TABLENAME)) {
                if (!admin.isTableDisabled(TABLENAME)) {
                    admin.disableTable(TABLENAME);
                }
                admin.deleteTable(TABLENAME);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 修改表
     */
    @Test
    public void modifyTable() {
        //获取表的描述器的对象
        try {
            HTableDescriptor ht = admin.getTableDescriptor(TABLENAME);
            //base_info列族已经存在,所以不用去new 直接获取
            HColumnDescriptor hc = ht.getFamily(Bytes.toBytes("base_info"));
            hc.setBlockCacheEnabled(false);

            //增加一个列族
//            HColumnDescriptor hc1=new HColumnDescriptor("other_info");
//            ht.addFamily(hc1);

//            HColumnDescriptor hc2 = new HColumnDescriptor("other_info");
//            ht.addFamily(hc2);

            HColumnDescriptor hc3 = new HColumnDescriptor("a_info:name");
            ht.addFamily(hc3);

            admin.modifyTable(TABLENAME, ht);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
