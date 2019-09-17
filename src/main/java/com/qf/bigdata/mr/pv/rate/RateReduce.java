package com.qf.bigdata.mr.pv.rate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RateReduce extends Reducer<Text,RateBean,Text,RateBean> {

    @Override
    protected void reduce(Text key, Iterable<RateBean> values, Context context) throws IOException, InterruptedException {
        List<RateBean> list=new ArrayList<RateBean>();
        for(RateBean value:values){
            list.add(new RateBean(value.getMovie(),value.getRate(),value.getTimeStamp(),value.getUid()));
        }
        Collections.sort(list);
        for(int i=0;i<10;i++){
            context.write(key,list.get(i));
        }
     }
}
