package com.csc.test.vehicle;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import com.csc.test.vehicle.parsers.AccelParser;

public class NaturalKeyPartitioner implements
		Partitioner<TimeseriesKey, AccelParser> {

	@Override
	public int getPartition(TimeseriesKey key, AccelParser value,
			int numPartitions) {
		return Math.abs(key.getTripId().hashCode() * 127) % numPartitions;
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}
}
