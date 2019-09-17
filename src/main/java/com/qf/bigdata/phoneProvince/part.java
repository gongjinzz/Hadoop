package com.qf.bigdata.phoneProvince;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class part extends Partitioner<Text,IntWritable> {

   static  HashMap<String,Integer> map=new HashMap<>();
    static {
        map.put("137",0);
        map.put("135",1);
        map.put("187",2);
    }
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String key=text.toString();
        Integer index=map.get(key.substring(0,3));
        return index==null?3:index;
    }
}
