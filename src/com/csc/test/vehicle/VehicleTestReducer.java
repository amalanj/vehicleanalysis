package com.csc.test.vehicle;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.csc.test.vehicle.parsers.AccelParser;

public class VehicleTestReducer extends MapReduceBase implements Reducer<TimeseriesKey, AccelParser, Text, Text> {

	static long sec_in_ms = 1000;

	private JobConf configuration;

	@Override
	public void configure(JobConf job) {

		this.configuration = job;

	}
	
	@Override
	public void reduce(TimeseriesKey key, Iterator<AccelParser> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
		AccelParser result = new AccelParser();
		AccelParser next_point = new AccelParser(); 
		result.setAccelData1Min(null);
		result.setAccelData1Max(null);
		result.setAccelData2Min(null);
		result.setAccelData2Max(null);
		result.setAccelData3Min(null);
		result.setAccelData3Max(null);
		
		int iWindowSizeInSec = this.configuration.getInt(
				"tv.floe.caduceus.hadoop.movingaverage.windowSize", 5);
		float iWindowStepSizeInSec = this.configuration.getFloat(
				"tv.floe.caduceus.hadoop.movingaverage.windowStepSize", 0.5f);

		long iWindowSizeInMS = iWindowSizeInSec * sec_in_ms;
		long iWindowStepSizeInMS = (long) (iWindowStepSizeInSec * sec_in_ms);

		Text out_key = new Text();
		Text out_val = new Text();

		SlidingWindow sliding_window = new SlidingWindow(iWindowSizeInMS,
				iWindowStepSizeInMS, sec_in_ms);

		while (values.hasNext()) {
			
			while (sliding_window.WindowIsFull() == false && values.hasNext()) {

				next_point = values.next();

				AccelParser p_copy = new AccelParser();
				p_copy.copy(next_point);

				try {
					sliding_window.AddPoint(p_copy);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
			if (sliding_window.WindowIsFull()) {

				LinkedList<AccelParser> oWindow = sliding_window
						.GetCurrentWindow();

				String strBackDate = oWindow.getLast().getDate();

				// ---------- compute the moving average here -----------

				out_key.set("Trip_Id: " + key.getTripId() + ", Date: "
						+ strBackDate);

				for (int x = 0; x < oWindow.size(); x++) {
					
					AccelParser parse = oWindow.get(x);
					
					if(result.getAccelData1Min() == null || 
							parse.getAccelData1Min().compareTo(result.getAccelData1Min()) < 0) {
		                result.setAccelData1Min(parse.getAccelData1Min());
		            } if(result.getAccelData1Max() == null || 
							parse.getAccelData1Max().compareTo(result.getAccelData1Max()) > 0) {
		                result.setAccelData1Max(parse.getAccelData1Max());
		            }
		            
		            if(result.getAccelData2Min() == null || 
							parse.getAccelData2Min().compareTo(result.getAccelData2Min()) < 0) {
		                result.setAccelData2Min(parse.getAccelData2Min());
		            } if(result.getAccelData2Max() == null || 
							parse.getAccelData2Max().compareTo(result.getAccelData2Max()) > 0) {
		                result.setAccelData2Max(parse.getAccelData2Max());
		            }
		            
		            if(result.getAccelData3Min() == null || 
							parse.getAccelData3Min().compareTo(result.getAccelData3Min()) < 0) {
		                result.setAccelData3Min(parse.getAccelData3Min());
		            } if(result.getAccelData3Max() == null || 
							parse.getAccelData3Max().compareTo(result.getAccelData3Max()) > 0) {
		                result.setAccelData3Max(parse.getAccelData3Max());
		            }
					

					//point_sum += oWindow.get(x).fValue;

				} // for

				//moving_avg = point_sum / oWindow.size();

				// out_val.set("Moving Average: " + moving_avg);

				String reduceOut = result.getAccelData1Min() + ", "+ result.getAccelData1Max() + ", " + result.getAccelData2Min()
						+ ", " + result.getAccelData2Max() + ", "+ result.getAccelData3Min() + ", " + result.getAccelData3Max();
				out_val.set(reduceOut);
				
				output.collect(out_key, out_val);

				// 2. step window forward

				sliding_window.SlideWindowForward();

			}
			
			
		}
		
		
		
	}

}
