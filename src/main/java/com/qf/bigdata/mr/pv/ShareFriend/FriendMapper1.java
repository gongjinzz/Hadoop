package com.qf.bigdata.mr.pv.ShareFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendMapper1 extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str=value.toString();
        String[] s=str.split(":");
        if(s.length==2){
        String[] ss=s[1].split(",");

        for(String a:ss){
            context.write(new Text(a),new Text(s[0]));
        }}
    }
}
