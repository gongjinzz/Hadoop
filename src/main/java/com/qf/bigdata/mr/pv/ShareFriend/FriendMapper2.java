package com.qf.bigdata.mr.pv.ShareFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FriendMapper2 extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line=value.toString();
        String[] str=line.split("\t");

        String[] ss=str[1].split(",");
        Arrays.sort(ss);
        for(int i=0;i<ss.length-1;i++){
            for(int j=i+1;j<ss.length;j++){
                context.write(new Text(ss[i]+"-"+ss[j]),new Text(str[0]));
            }
        }
    }
}
