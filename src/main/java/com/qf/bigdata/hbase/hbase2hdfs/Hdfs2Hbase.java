package com.qf.bigdata.hbase.hbase2hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Iterator;

public class Hdfs2Hbase {

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text k=new Text();
    private LongWritable v=new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] columns=line.split("\t");
            if(columns[0].contains("age")){
                String[] kv=columns[0].split(":");
                k.set(kv[1]);
                context.write(k,v);
            }
        }
    }

    static class MyReducer extends TableReducer<Text,LongWritable,ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count=0L;
            Iterator<LongWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                LongWritable next = iterator.next();
                count+=next.get();
            }

            Put put=new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age"),Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("ageCount"),Bytes.toBytes(count+""));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf=new Configuration();
        conf.set("hbase.zookeeper.quorum","mini1:2181,mini2:2181,mini3:2181");
        conf.set("fs.defaultFS","hdfs://mini1:9000");
        Job job=Job.getInstance(conf);
        job.setJarByClass(Hdfs2Hbase.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        TableMapReduceUtil.initTableReducerJob(
                "ns1:result",
                MyReducer.class,
                job
        );

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        boolean res=job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
