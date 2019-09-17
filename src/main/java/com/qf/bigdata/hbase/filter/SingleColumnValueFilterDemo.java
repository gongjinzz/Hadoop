package com.qf.bigdata.hbase.filter;

import com.qf.bigdata.hbase.util.HbaseTools;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class SingleColumnValueFilterDemo {

    public static void main(String[] args) throws IOException {

        //1 创建正则比较器
        RegexStringComparator rsc = new RegexStringComparator("^.+$");
        //2 创建子串比较器
        SubstringComparator sc = new SubstringComparator("a");
        //3 创建二进制比较器
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("lis"));
        //4 创建二进制前缀比较器
        BinaryPrefixComparator bpc = new BinaryPrefixComparator(Bytes.toBytes("lis"));

        //创建过滤器
        SingleColumnValueFilter sf = new SingleColumnValueFilter(
                Bytes.toBytes("base_info"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                bpc
        );
        sf.setFilterIfMissing(true);
        FilterList filterList=new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(sf);
        Scan scan=new Scan();
        scan.setFilter(filterList);
        Table table= HbaseTools.getTable(TableName.valueOf("ns1:demo"));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result rs = iterator.next();
            System.out.println(Bytes.toString(rs.getRow()));
            while(rs.advance()){
                Cell current = rs.current();
                System.out.print("\t"+new String(CellUtil.cloneFamily(current),"utf-8"));
                System.out.print(":"+new String(CellUtil.cloneQualifier(current),"utf-8"));
                System.out.print("\t"+new String(CellUtil.cloneValue(current),"utf-8"));
            }
            System.out.println("---------------------------------------");
        }

    }
}
