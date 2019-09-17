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

public class TableDemo3 {

    private static Admin admin=null;
    private static TableName TABLENAME=TableName.valueOf("ns3:demo4");

    @Before
    public void init(){
        admin= HbaseTools.getAdmin();
    }

    @After
    public  void  closeAdmin(){
        HbaseTools.closeAdmin(admin);
    }

    @Test
    public void createTable(){
        HTableDescriptor ht=new HTableDescriptor(TABLENAME);
        HColumnDescriptor hc=new HColumnDescriptor(Bytes.toBytes("base_info"));
        ht.addFamily(hc);
        try {
            admin.createTable(ht);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     自改一个表
     */
    @Test
    public void modifyTable(){
        try {
            System.out.println("修改demo4表的属性");
            HTableDescriptor ht=admin.getTableDescriptor(TABLENAME);
            HColumnDescriptor hc=new HColumnDescriptor(Bytes.toBytes("extra_info"));
            ht.addFamily(hc);
            System.out.println("增加了一列extro_info");
            hc.setBlockCacheEnabled(false);
            System.out.println("extro_info的blockcache改为了false");
            admin.modifyTable(TABLENAME,ht);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一个表
     */
    @Test
    public void  deleteTable(){
        admin=HbaseTools.getAdmin();
        try {
            if(admin.tableExists(TABLENAME)){
                if(!admin.isTableDisabled(TABLENAME)){
                    admin.disableTable(TABLENAME);
                }
                admin.deleteTable(TABLENAME);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
