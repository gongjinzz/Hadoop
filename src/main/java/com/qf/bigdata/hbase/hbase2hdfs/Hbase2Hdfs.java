package com.qf.bigdata.hbase.hbase2hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Hbase2Hdfs {

    static class MyMapper extends TableMapper<Text, NullWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            CellScanner cellScanner = value.cellScanner();
            String str = "";
            while (cellScanner.advance()) {
                Cell current = cellScanner.current();
                str += new String(CellUtil.cloneQualifier(current), "utf-8");
                str += ":" + new String(CellUtil.cloneValue(current), "utf-8") + "\t";
            }
            context.write(new Text(str), NullWritable.get());
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "mini1:2181,mini2:2181,mini3:2181");
        conf.set("fs.defaultFS", "hdfs://mini1:9000");
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(Hbase2Hdfs.class);

            TableMapReduceUtil.initTableMapperJob(
                    "ns1:demo",
                    getScan(),
                    MyMapper.class,
                    Text.class,
                    NullWritable.class,
                    job
            );

            //设置输出
            FileOutputFormat.setOutputPath(job, new Path(args[0]));
            boolean res = job.waitForCompletion(true);
            System.exit(res ? 0 : 1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Scan getScan() {
        Scan scan = new Scan();
        return scan;
    }


}
