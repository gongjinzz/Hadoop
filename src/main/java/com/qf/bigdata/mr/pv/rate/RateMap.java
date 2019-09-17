package com.qf.bigdata.mr.pv.rate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class RateMap extends Mapper<LongWritable,Text,Text,RateBean>{

    ObjectMapper om;
    Text k=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        om=new ObjectMapper();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            RateBean bean=om.readValue(value.toString(),RateBean.class);
            k.set(bean.getUid());
            context.write(k,bean);

    }
}
