package com.qf.bigdata.mr.pv.top10watch;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AMap extends Mapper<LongWritable, IntWritable, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        String line = key.toString();
        String[] strs = line.split("\t");
        String str = "";
        if (strs.length >= 9) {
            strs[3] = strs[3].replace(" & ", ",");
            for (int i = 0; i < 9; i++) {
                if (i == 8) {
                    str += strs[i];
                } else {
                    str += strs[i] + "\t";
                }
            }
        }
        if (strs.length >= 10) {
            for (int i = 9; i <= strs.length - 1; i++) {
                if (i == 9) {
                    str += "\t";
                } else if (i == str.length() - 1) {
                    str += strs[i];
                } else {
                    str += strs[i] + ",";
                }
            }
        }
        context.write(new Text(str),NullWritable.get());
    }
}
