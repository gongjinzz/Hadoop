package com.qf.bigdata.mr.pv.ShareFriend;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JobSubmitter {

//    public static void main(String[] args) {
//
//        Configuration conf=new Configuration();
//
//        try {
//            Job job1=Job.getInstance(conf);
//           // job1.setJarByClass(JobSubmitter.class);
//            job1.setJar("/root/fs.jar");
//
//            job1.setMapperClass(FriendMapper1.class);
//            job1.setReducerClass(FriendReducer1.class);
//            job1.setMapOutputKeyClass(Text.class);
//            job1.setMapOutputValueClass(Text.class);
//            job1.setOutputKeyClass(Text.class);
//            job1.setOutputValueClass(Text.class);
//
//            job1.setInputFormatClass(TextInputFormat.class);
//            job1.setOutputFormatClass(TextOutputFormat.class);
//
//            Path in=new Path("/in");
//            Path out=new Path("/out2");
//
//            FileInputFormat.addInputPath(job1,in);
//            FileOutputFormat.setOutputPath(job1,out);
//
//
//            boolean res=job1.waitForCompletion(true);
//            System.exit(res?0:1);
//
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//
//    }
public static void main(String[] args) {

    Configuration conf=new Configuration();

    try {
        Job job1=Job.getInstance(conf);
        // job1.setJarByClass(JobSubmitter.class);
        job1.setJar("/root/fs.jar");

        job1.setMapperClass(FriendMapper2.class);
        job1.setReducerClass(FriendReducer2.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        Path in=new Path("/out2");
        Path out=new Path("/out3");

        FileInputFormat.addInputPath(job1,in);
        FileOutputFormat.setOutputPath(job1,out);


        boolean res=job1.waitForCompletion(true);
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
