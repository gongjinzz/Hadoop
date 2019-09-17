package com.qf.bigdata.mr.DataProcressing;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AReduce extends Reducer<Text,NullWritable,Text,NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for(NullWritable value:values){
            context.write(key,NullWritable.get());
        }
    }
}
