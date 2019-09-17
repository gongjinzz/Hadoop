package com.qf.bigdata.hbase.filter;

import com.qf.bigdata.hbase.util.HbaseTools;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class PageFilterDemo {

    public static void main(String[] args) throws IOException {

        PageFilter pf = new PageFilter(2);
        Scan scan = new Scan();
        scan.setFilter(pf);
        Table table = HbaseTools.getTable(TableName.valueOf("ns1:demo"));

        int count = 0;
        String maxKey = "";

        while (true) {
            count = 0;
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> it = scanner.iterator();
            while (it.hasNext()){
                count+=1;
                Result rs = it.next();
                maxKey= Bytes.toString(rs.getRow());
                while(rs.advance()){
                    Cell current = rs.current();
                    System.out.print(new String(CellUtil.cloneFamily(current),"utf-8"));
                    System.out.print(":"+new String(CellUtil.cloneQualifier(current),"utf-8"));
                    System.out.print("\t"+new String(CellUtil.cloneValue(current),"utf-8"));
                }
                System.out.println(" ");
            }
            if(count<2){
               break;
            }
            scan.setStartRow(Bytes.toBytes(maxKey+"\001"));
            System.out.println("新的一页开始了");

        }
    }
}
