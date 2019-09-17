package com.qf.bigdata.mr.pv.rate;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RateBean implements WritableComparable<RateBean> {

    private String movie;
    private String rate;
    private String timeStamp;
    private String uid;

    public RateBean(String movie, String rate, String timeStamp, String uid) {
        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
        this.uid = uid;
    }

    public RateBean() {

    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "movie" + this.movie + " rate:" + this.rate;
    }

    @Override
    public int compareTo(RateBean o) {
        return -(this.rate.compareTo(o.getRate()));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.movie);
        out.writeUTF(this.rate);
        out.writeUTF(this.timeStamp);
        out.writeUTF(this.uid);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.movie=in.readUTF();
        this.rate=in.readUTF();
        this.timeStamp=in.readUTF();
        this.uid=in.readUTF();
    }
}
