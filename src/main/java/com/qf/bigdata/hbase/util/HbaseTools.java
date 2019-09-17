package com.qf.bigdata.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HbaseTools {

    private static Logger logger=Logger.getLogger(HbaseTools.class);
    private static final String Connection_KEY="hbase.zookeeper.quorum";
    private static final String Connection_VALUE="mini1:2181,mini2:2181,mini3:2181";
    private static Connection conn=null;
    private static Table table= null;

    static {
        Configuration conf=new Configuration();
        conf.set(Connection_KEY,Connection_VALUE);
        try {
            conn= ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
           logger.error("获取hbase连接失败");
        }
    }

    public static Admin getAdmin(){
        Admin admin=null;
        try {
            admin=conn.getAdmin();
        } catch (IOException e) {
            logger.error("获取admin对象异常");
        }
        return admin;
    }

    public static void closeAdmin(Admin admin){
        if(admin!=null){
            try {
                admin.close();
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static  Table getTable(TableName tableName){

        try {
            table = conn.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public static void closeTable(){
        if(table!=null){
            try {
                table.close();
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
