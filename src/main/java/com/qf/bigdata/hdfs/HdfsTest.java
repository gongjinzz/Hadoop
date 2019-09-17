package com.qf.bigdata.hdfs;


import com.google.common.annotations.Beta;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


public class HdfsTest {

    private static Logger logger = Logger.getLogger(HdfsTest.class);
    FileSystem fs = null;

    //alt +enter导包
    @Before
    public void init() {
        fs = HdfsUtil.getClient();
    }

    /**
     * 查看文件列表的测试
     */
    @Test
    public void testListFiles() {
        try {
            fs.mkdirs(new Path("/a/b/c"));
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
            System.out.println(listFiles);

            while (listFiles.hasNext()) {

                LocatedFileStatus fileStatus = listFiles.next();
                System.out.println(fileStatus.getPath().getName());
                System.out.println(fileStatus.getBlockSize() / 1024 / 1024 + "M");
                System.out.println(fileStatus.getPermission());
                BlockLocation[] blockLocations = fileStatus.getBlockLocations();
                for (BlockLocation bl : blockLocations) {
                    System.out.println("block-length" + bl.getLength() / 1024 / 1024);
                    System.out.println("block-offset" + bl.getOffset() / 1024 / 1024);
                    String[] hosts = bl.getHosts();
                    for (String host : hosts) {
                        System.out.println(host);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testListFile2() {
        try {
            RemoteIterator<LocatedFileStatus> listFile = fs.listFiles(new Path("/"), true);
            while (listFile.hasNext()) {

                LocatedFileStatus lfs = listFile.next();
                System.out.println(lfs.getPath().getName());
                System.out.println(lfs.getBlockSize() / 1024 / 1024 + "M");
                System.out.println(lfs.getPermission());
                BlockLocation[] blockLocations = lfs.getBlockLocations();
                for (BlockLocation bl : blockLocations) {
                    System.out.println("length" + bl.getLength() / 1024 / 1024);
                    System.out.println("offset" + bl.getOffset() / 1024 / 1024);

                    String[] hosts = bl.getHosts();
                    for (String host : hosts) {
                        System.out.println(host);
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCat() {
        try {
            FSDataInputStream in = fs.open(new Path("/tmp/test.txt"));
            //拿到文件信息
            FileStatus[] fileStatuses = fs.listStatus(new Path("/tmp/hadoop安装包.zip"));
            //获取这个文件的所有block信息
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatuses[0], 0L, fileStatuses[0].getLen());
            //第一个block的长度
            long length = fileBlockLocations[0].getLength();
            //第一个block的起始偏移量
            long offset = fileBlockLocations[0].getOffset();

            System.out.println(length);
            System.out.println(offset);

            //获取第一block写入输出流
            IOUtils.copyBytes(in,System.out,(int)length);
            byte[] b=new byte[4096];

            FileOutputStream os = new FileOutputStream(new File("d:/block0"));
            while(in.read(offset, b, 0, 4096)!=-1){
                os.write(b);
                offset += 4096;
                if(offset>=length) return;
            };
            os.flush();
            os.close();
            in.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


//        try {
//            fs.copyFromLocalFile(new Path("D:\\test.txt"),new Path("/tmp"));
//            System.out.println("上传成功");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
