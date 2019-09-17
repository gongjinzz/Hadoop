package com.qf.bigdata.hbase.namespace;


import com.qf.bigdata.hbase.util.HbaseTools;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class NSDemo {

    private static Logger logger = Logger.getLogger(NSDemo.class);
    private Admin admin = null;

    @Before
    public void init() {
         admin= HbaseTools.getAdmin();
    }
    @After
    public void closeAdmin(){
        HbaseTools.closeAdmin(admin);
    }

    /**
     * 列出所有的名称空间
     */
    @Test
    public void listNamespace(){
        try {
            NamespaceDescriptor[] listNamespaceDescriptors = admin.listNamespaceDescriptors();
            for(NamespaceDescriptor namespaceDescriptor:listNamespaceDescriptors){
                System.out.println(namespaceDescriptor.getName());
            }
            System.out.println("打印完成");
        } catch (IOException e) {
            logger.error("打印失败");
        }
    }

    /**
     * 获取所有的表
     */
    @Test
    public void listTables(){
        try {
            TableName[] tableNames = admin.listTableNames();
            for(TableName tableName:tableNames){
                System.out.println(Bytes.toString(tableName.getName()));

                System.out.println(tableName.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取某个namespace下的表
     */
    @Test
    public void listNamespaceTables(){
        try {
            TableName[] tableNames = admin.listTableNamesByNamespace("ns1");
            for(TableName tableName:tableNames){
                System.out.println(tableName.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除名称空间
     */
    @Test
    public void deleteNamespace(){

        try {
            HTableDescriptor[] ns1s = admin.listTableDescriptorsByNamespace("ns1");
            if(ns1s.length>0){
                System.out.println("请先删除ns1内的表");
            }else{
                admin.deleteNamespace("ns1");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
