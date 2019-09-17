package com.qf.bigdata.mr.pv.ShareFriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReducer1 extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String str="";
        for(Text value:values){
               str+=value+",";
        }
        str=str.substring(0,str.length()-1);
        context.write(key,new Text(str));
    }


}
