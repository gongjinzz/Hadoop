package com.qf.bigdata.mr.pv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;

public class PvReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    IntWritable v=new IntWritable();

    //一组数据调用一次reduce,因为是一组所以ip地址相同
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count=0;
        for(IntWritable value:values){
            //value是IntWriterabe类型通过get方法编程int类型
            count+=value.get();
        }
        v.set(count);
        context.write(key,v);
        //需要把map程序和reduce程序交给yarn去处理
        //所以需要写一个yarn的客户端程序，然后将程序打包，提交到集群上运行
    }
}
