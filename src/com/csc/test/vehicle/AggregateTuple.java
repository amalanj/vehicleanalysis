package com.csc.test.vehicle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AggregateTuple {
	
	private Integer min;
    private Integer max;
    private Integer avg;

    public AggregateTuple(){
        this.min=0;
        this.max=0;
        this.avg=0;
    }

    public Integer getMin(){
        return min;
    }
    public void setMin(Integer min){
        this.min=min;
    }
    public Integer getMax(){
        return max;
    }
    public void setMax(Integer max){
        this.max=max;
    }
    public long getAvg(){
        return avg;
    }
    public void setAvg(Integer avg){
        this.avg=avg;
    }
    public void readFields(DataInput in) throws IOException {
        min = in.readInt();
        max = in.readInt();
        avg = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(min);
        out.writeInt(max);
        out.writeInt(avg);
    }

    public String toString() {
        return min + "\t" + max + "\t" + avg;
    }
    

}
