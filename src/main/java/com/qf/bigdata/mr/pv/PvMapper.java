package com.qf.bigdata.mr.pv;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.iq80.leveldb.DB;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;


public class PvMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//
//        String line = value.toString();
//        String[] strs = line.split("\t");
//
//        int start = (int) new Date().getTime();
//        String str = "";
//        if (strs.length >= 9) {
//            strs[3] = strs[3].replaceAll(" & ", ",");
//            for (int i = 0; i < 9; i++) {
//                if (i == 8) {
//                    str += strs[i];
//                } else {
//                    str += strs[i] + " ";
//                }
//            }
//            if (strs.length >= 10) {
//                str+=" ";
//                int j = 10;
//                while (j <= strs.length) {
//                    if (j == strs.length - 1) {
//                        str += strs[j - 1];
//                        j++;
//                    } else {
//                        str += strs[j - 1] + ",";
//                        j++;
//                    }
//                }
//            }
//        }
//        int end = (int) new Date().getTime();
//        context.write(new Text(str), new IntWritable(1));
//    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line=value.toString();
        line.split("\t");
    }
}
