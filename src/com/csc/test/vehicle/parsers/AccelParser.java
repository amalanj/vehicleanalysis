package com.csc.test.vehicle.parsers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Writable;


public class AccelParser implements Writable, Comparable<AccelParser>{
	
	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";

	private static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

	private String tripId;

	private long eventTime;
	
	private String accelData1;
	
	private String accelData2;
	
	private String accelData3;

	private String accelData1Min;

	private String accelData1Max;

	private String accelData2Min;

	private String accelData2Max;

	private String accelData3Min;

	private String accelData3Max;

	public String getTripId() {
		return tripId;
	}

	public long getEventTime() {
		return eventTime;
	}

	public String getAccelData1Min() {
		return accelData1Min;
	}

	public String getAccelData1Max() {
		return accelData1Max;
	}

	public String getAccelData2Min() {
		return accelData2Min;
	}

	public String getAccelData2Max() {
		return accelData2Max;
	}

	public String getAccelData3Min() {
		return accelData3Min;
	}

	public String getAccelData3Max() {
		return accelData3Max;
	}

	public void setTripId(String tripId) {
		this.tripId = tripId;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	public void setAccelData1Min(String accelData1Min) {
		this.accelData1Min = accelData1Min;
	}

	public void setAccelData1Max(String accelData1Max) {
		this.accelData1Max = accelData1Max;
	}

	public void setAccelData2Min(String accelData2Min) {
		this.accelData2Min = accelData2Min;
	}

	public void setAccelData2Max(String accelData2Max) {
		this.accelData2Max = accelData2Max;
	}

	public void setAccelData3Min(String accelData3Min) {
		this.accelData3Min = accelData3Min;
	}

	public void setAccelData3Max(String accelData3Max) {
		this.accelData3Max = accelData3Max;
	}

	public String getAccelData1() {
		return accelData1;
	}

	public String getAccelData2() {
		return accelData2;
	}

	public String getAccelData3() {
		return accelData3;
	}

	public void setAccelData1(String accelData1) {
		this.accelData1 = accelData1;
	}

	public void setAccelData2(String accelData2) {
		this.accelData2 = accelData2;
	}

	public void setAccelData3(String accelData3) {
		this.accelData3 = accelData3;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		//this.tripId = in.readUTF();
		this.eventTime = in.readLong();
		this.accelData1Min = in.readUTF();
		this.accelData1Max = in.readUTF();
		this.accelData2Min = in.readUTF();
		this.accelData2Max = in.readUTF();
		this.accelData3Min = in.readUTF();
		this.accelData3Max = in.readUTF();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		//out.writeUTF(this.tripId);
		out.writeLong(this.eventTime);
		out.writeUTF(this.accelData1Min);
		out.writeUTF(this.accelData1Max);
		out.writeUTF(this.accelData2Min);
		out.writeUTF(this.accelData2Max);
		out.writeUTF(this.accelData3Min);
		out.writeUTF(this.accelData3Max);
		
	}

	@Override
	public int compareTo(AccelParser oOther) {
		if (this.eventTime < oOther.eventTime) {
			return -1;
		} else if (this.eventTime > oOther.eventTime) {
			return 1;
		}

		// default -- they are equal
		return 0;
	}

	public void copy(AccelParser source) {

		this.accelData1Min = source.accelData1Min;
		this.accelData1Max = source.accelData1Max;
		this.accelData1Min = source.accelData1Min;
		this.accelData1Max = source.accelData1Max;
		this.accelData3Min = source.accelData3Min;
		this.accelData3Max = source.accelData3Max;

	}

	public String getDate() {
		return sdf.format(this.eventTime);
	}
	
	

}
