package com.qf.bigdata.mr.DataProcressing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AMap extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strs = line.split("\t");
        String str = null;
        if (strs.length >= 9) {
            strs[2] = strs[2].replace(" & ", ",");
            for (int i = 0; i < 9; i++) {
                str += strs[i] + "\t";
            }
        }
        String relateid = null;
        if (strs.length >= 10) {
            for (int i = 9; i <= strs.length - 1; i++) {
                relateid+=strs[i]+",";
            }
            relateid=relateid.substring(0,relateid.length()-1);
        }
        str+=relateid;
        context.write(new Text(str),NullWritable.get());
    }
}
