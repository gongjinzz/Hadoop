package com.qf.bigdata.phoneProvince;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JobSubmit {

    public static void main(String[] args) {

        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        try {
            Job job=Job.getInstance(conf);
            job.setJarByClass(JobSubmit.class);

            job.setMapperClass(AMap.class);
            job.setReducerClass(AReducer.class);



            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setPartitionerClass(part.class);
            job.setNumReduceTasks(4);
            job.setGroupingComparatorClass(Group.class);

            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job,new Path("D:\\inn\\phonr.txt"));

            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job,new Path("D:\\out4"));

            boolean res=job.waitForCompletion(true);
            System.exit(res?0:1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
