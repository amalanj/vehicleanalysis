package com.csc.test.vehicle.parsers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Writable;


public class AccelParser implements Writable, Comparable<AccelParser>{
	
	//private static final String DATE_FORMAT = "M/d/yyyy";
	
	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; //yyyy-MM-dd HH:mm:ss:SSS

	private static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

	private String tripId;

	private long eventTime;

	private Integer accelData1Min;

	private Integer accelData1Max;

	private Integer accelData2Min;

	private Integer accelData2Max;

	private Integer accelData3Min;

	private Integer accelData3Max;

	
	public String getTripId() {
		return tripId;
	}

	public long getEventTime() {
		return eventTime;
	}

	public Integer getAccelData1Min() {
		return accelData1Min;
	}

	public Integer getAccelData1Max() {
		return accelData1Max;
	}

	public Integer getAccelData2Min() {
		return accelData2Min;
	}

	public Integer getAccelData2Max() {
		return accelData2Max;
	}

	public Integer getAccelData3Min() {
		return accelData3Min;
	}

	public Integer getAccelData3Max() {
		return accelData3Max;
	}

	public void setTripId(String tripId) {
		this.tripId = tripId;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	public void setAccelData1Min(Integer accelData1Min) {
		this.accelData1Min = accelData1Min;
	}

	public void setAccelData1Max(Integer accelData1Max) {
		this.accelData1Max = accelData1Max;
	}

	public void setAccelData2Min(Integer accelData2Min) {
		this.accelData2Min = accelData2Min;
	}

	public void setAccelData2Max(Integer accelData2Max) {
		this.accelData2Max = accelData2Max;
	}

	public void setAccelData3Min(Integer accelData3Min) {
		this.accelData3Min = accelData3Min;
	}

	public void setAccelData3Max(Integer accelData3Max) {
		this.accelData3Max = accelData3Max;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		//this.tripId = in.readUTF();
		this.eventTime = in.readLong();
		this.accelData1Min = in.readInt();
		this.accelData1Max = in.readInt();
		this.accelData2Min = in.readInt();
		this.accelData2Max = in.readInt();
		this.accelData3Min = in.readInt();
		this.accelData3Max = in.readInt();
		
	}
	
	/**
	 * This is a static method that deserializes a point from the underlying
	 * binary representation.
	 * 
	 * @param in
	 *            A DataInput object that represents the underlying stream to
	 *            read from.
	 * @return A AccelParser
	 * @throws IOException
	 */
	public static AccelParser read(DataInput in) throws IOException {

		AccelParser p = new AccelParser();
		p.readFields(in);
		return p;

	}	

	@Override
	public void write(DataOutput out) throws IOException {
		
		//out.writeInt(this.tripId);
		out.writeLong(this.eventTime);
		out.writeInt(this.accelData1Min);
		out.writeInt(this.accelData1Max);
		out.writeInt(this.accelData2Min);
		out.writeInt(this.accelData2Max);
		out.writeInt(this.accelData3Min);
		out.writeInt(this.accelData3Max);
		
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
		this.accelData2Min = source.accelData2Min;
		this.accelData2Max = source.accelData2Max;
		this.accelData3Min = source.accelData3Min;
		this.accelData3Max = source.accelData3Max;
		this.eventTime = source.eventTime;		

	}

	public String getDate() {
		return sdf.format(this.eventTime);
	}
	
	

}
