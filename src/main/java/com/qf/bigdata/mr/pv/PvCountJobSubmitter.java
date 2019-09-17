package com.qf.bigdata.mr.pv;

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

public class PvCountJobSubmitter {

    public static void main(String[] args) {

        /**
         * job需要一些配置信息
         */
        Configuration conf = new Configuration();
        /**
         * 程序启动时需要做一些参数设置，要告诉程序中的一些信息
         * ，这些信息很零散，要把这些信息封装到一个对象中
         * 这个对象就是job
         */
        try {
            Job job = Job.getInstance(conf);
            //job要封装的信息包括
            //执行程序的jar包的指定
            //告诉程序jar包在/root目录下名字为pv.jar
            //pv.jar就是我们打包后的jar包的名字
            job.setJar("/root/pv.jar");
            /**
             *告诉框架用什么组件去读数据,普通的文本文件用TextInputFormat去处理
             */
            job.setInputFormatClass(TextInputFormat.class);
            /**
             * 告诉组件去哪儿读数据
             * TextInputFormat有一个父类FileInputFormat
             * 用父类去指定去哪儿读数据
             * job:给哪儿个job设置输入路径
             * path:输入路径是哪儿
             */
            Path in=new Path(args[0]);
            FileInputFormat.addInputPath(job,in);
            /**
             * 一个jar包中可能有多个job,我们需要指定这个job使用的
             * map类是哪一个,reduce类是哪一个
             */
            job.setMapperClass(PvMapper.class);
            job.setReducerClass(PvReducer.class);
            /**
             * 告诉框架map逻辑返回给框架的结果的数据类型是什么
             * 这样框架才能帮我们去序列化
             */
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            /**
             * 告诉框架用什么组件写数据
             * 要写文本文件,写出的类就是TextOutputFormat
             */
            job.setOutputFormatClass(TextOutputFormat.class);
            /**
             * 指定写出数据的输出路径
             * 用TextOutputFormatde 父类去指定
             */
            Path out=new Path(args[1]);
            FileOutputFormat.setOutputPath(job, out);

            try {
                //waitForCompletion会将jar包和信息交给RM,RM会将jar包分发
                //将程序启动起来
                //参数如果传递的是true,MR程序在运行的时候会将进度打印到客户端
                boolean res = job.waitForCompletion(true);
                //程序有状态码的返回
                System.exit(res ? 0 : 1);
            //打包运行
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
