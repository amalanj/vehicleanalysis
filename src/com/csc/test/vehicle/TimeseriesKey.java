package com.csc.test.vehicle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class TimeseriesKey implements WritableComparable<TimeseriesKey> {

	private String TripId = "";
	private long Timestamp = 0;

	public void set(String strTripId, long lTS) {

		this.TripId = strTripId;
		this.Timestamp = lTS;

	}

	public String getTripId() {
		return this.TripId;
	}

	public long getTimestamp() {
		return this.Timestamp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.TripId = in.readUTF();
		this.Timestamp = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(TripId);
		out.writeLong(this.Timestamp);
	}

	@Override
	public int compareTo(TimeseriesKey other) {

		if (this.TripId.compareTo(other.TripId) != 0) {
			return this.TripId.compareTo(other.TripId);
		} else if (this.Timestamp != other.Timestamp) {
			return Timestamp < other.Timestamp ? -1 : 1;
		} else {
			return 0;
		}

	}

	public static class TimeSeriesKeyComparator extends WritableComparator {
		public TimeSeriesKeyComparator() {
			super(TimeseriesKey.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(TimeseriesKey.class,
				new TimeSeriesKeyComparator());
	}

}

