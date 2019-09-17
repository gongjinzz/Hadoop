package com.qf.bigdata.hbase.filter;

import com.qf.bigdata.hbase.util.HbaseTools;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class FilterListDemo {

    //过滤器链
    public static void main(String[] args) throws IOException {

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter sf = new SingleColumnValueFilter(
                Bytes.toBytes("base_info"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.LESS,
                Bytes.toBytes("25")
        );

        SingleColumnValueFilter sf1 = new SingleColumnValueFilter(
                Bytes.toBytes("base_info"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.GREATER,
                Bytes.toBytes("z")
        );

        sf.setFilterIfMissing(true);
        sf1.setFilterIfMissing(true);

        filterList.addFilter(sf);
        filterList.addFilter(sf1);

        Scan scan = new Scan();
        scan.setFilter(filterList);
        Table table = HbaseTools.getTable(TableName.valueOf("ns1:demo"));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result rs = iterator.next();
            CellScanner cellScanner = rs.cellScanner();
            System.out.println(Bytes.toString(rs.getRow()));
            while (cellScanner.advance()) {
                Cell current = cellScanner.current();
                System.out.print(new String(CellUtil.cloneFamily(current),"utf-8"));
                System.out.print(":"+new String(CellUtil.cloneQualifier(current),"utf-8"));
                System.out.print("\t"+new String(CellUtil.cloneValue(current),"utf-8"));
            }
        }


    }
}
