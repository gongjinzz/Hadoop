package com.qf.bigdata.phoneProvince;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Group extends WritableComparator{

    public Group(){
        super(Text.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String str1=a.toString();
        String str2=b.toString();

        return str1.substring(0,1).compareTo(str2.substring(0,1));
    }
}
