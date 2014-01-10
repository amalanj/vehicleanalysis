package com.csc.test.vehicle;

import java.io.IOException;
import java.text.SimpleDateFormat;
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
	
	static enum PointCounters {
		POINTS_SEEN, POINTS_ADDED_TO_WINDOWS, MOVING_AVERAGES_CALCD
	};
	
	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; //yyyy-MM-dd HH:mm:ss:SSS

	private static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

	static long sec_in_ms = 1000;
	
	//static long days_in_ms = 1000*60*60*24;

	private JobConf configuration;

	@Override
	public void configure(JobConf job) {

		this.configuration = job;

	}
	
	@Override
	public void reduce(TimeseriesKey key, Iterator<AccelParser> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
		//float point_sum = 0;
		//float moving_avg = 0;
				
		AccelParser next_point = new AccelParser(); 
				
		int iWindowSizeInSec = 20;
		int iWindowStepSizeInSec = 1;

		long iWindowSizeInMS = iWindowSizeInSec * sec_in_ms;
		long iWindowStepSizeInMS = iWindowStepSizeInSec * sec_in_ms;
		
		//long iWindowSizeInMS = iWindowSizeInSec * days_in_ms;
		//long iWindowStepSizeInMS = iWindowStepSizeInSec * days_in_ms;

		Text out_key = new Text();
		Text out_val = new Text();

		SlidingWindow sliding_window = new SlidingWindow(iWindowSizeInMS,
				iWindowStepSizeInMS, sec_in_ms);
		/*SlidingWindow sliding_window = new SlidingWindow(iWindowSizeInMS,
				iWindowStepSizeInMS, days_in_ms);*/

		while (values.hasNext()) {
			
			while (sliding_window.WindowIsFull() == false && values.hasNext()) {
				
				next_point = values.next();

				reporter.incrCounter(PointCounters.POINTS_ADDED_TO_WINDOWS, 1);

				AccelParser p_copy = new AccelParser();
				p_copy.copy(next_point);

				try {
					sliding_window.AddPoint(p_copy);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
			if (sliding_window.WindowIsFull() || !values.hasNext()) {
				
				reporter.incrCounter(PointCounters.MOVING_AVERAGES_CALCD, 1);

				String reduceOut = new String();
				
				AccelParser result = new AccelParser();
						
				LinkedList<AccelParser> oWindow = sliding_window
						.GetCurrentWindow();

				String strBackDate = oWindow.getLast().getDate();

				out_key.set("Trip_Id: " + key.getTripId() + ", Date: "
						+ strBackDate + ", oWindow Size: "+oWindow.size());
				//out_key.set("Trip_Id: " + key.getTripId() + ", Date: " + sdf.format(key.getTimestamp()));
				
				result.setAccelData1Min(null);
				result.setAccelData1Max(null);
				result.setAccelData2Min(null);
				result.setAccelData2Max(null);
				result.setAccelData3Min(null);
				result.setAccelData3Max(null);
				
				for (int x = 0; x < oWindow.size(); x++) {
					
					if(result.getAccelData1Min() == null || 
							oWindow.get(x).getAccelData1Min().compareTo(result.getAccelData1Min()) < 0) {
		                result.setAccelData1Min(oWindow.get(x).getAccelData1Min());
		            } if(result.getAccelData1Max() == null || 
		            		oWindow.get(x).getAccelData1Max().compareTo(result.getAccelData1Max()) > 0) {
		                result.setAccelData1Max(oWindow.get(x).getAccelData1Max());
		            }
		            
		            if(result.getAccelData2Min() == null || 
		            		oWindow.get(x).getAccelData2Min().compareTo(result.getAccelData2Min()) < 0) {
		                result.setAccelData2Min(oWindow.get(x).getAccelData2Min());
		            } if(result.getAccelData2Max() == null || 
		            		oWindow.get(x).getAccelData2Max().compareTo(result.getAccelData2Max()) > 0) {
		                result.setAccelData2Max(oWindow.get(x).getAccelData2Max());
		            }
		            
		            if(result.getAccelData3Min() == null || 
		            		oWindow.get(x).getAccelData3Min().compareTo(result.getAccelData3Min()) < 0) {
		                result.setAccelData3Min(oWindow.get(x).getAccelData3Min());
		            } if(result.getAccelData3Max() == null || 
		            		oWindow.get(x).getAccelData3Max().compareTo(result.getAccelData3Max()) > 0) {
		                result.setAccelData3Max(oWindow.get(x).getAccelData3Max());
		            }
					
				} // for

				//moving_avg = point_sum / oWindow.size();

				//out_val.set("AccelData1Min Moving Average: " + moving_avg);

				reduceOut = result.getAccelData1Min() + ", "+ result.getAccelData1Max() + ", " + result.getAccelData2Min()
						+ ", " + result.getAccelData2Max() + ", "+ result.getAccelData3Min() + ", " + result.getAccelData3Max();
				out_val.set(reduceOut);
				
				output.collect(out_key, out_val);

				// 2. step window forward
				
				//sliding_window.SlideWindowForward();
				
				sliding_window = new SlidingWindow(iWindowSizeInMS,iWindowStepSizeInMS, sec_in_ms);

			}
			
			
		}
		
		/*String reduceOut = new String();
		while (values.hasNext()) {			
			next_point = values.next();		
			reduceOut = next_point.getAccelData1Min() + ", "+ next_point.getAccelData1Max() + ", " + next_point.getAccelData2Min()
					+ ", " + next_point.getAccelData2Max() + ", "+ next_point.getAccelData3Min() + ", " + next_point.getAccelData3Max();
			out_val.set(reduceOut);			
			output.collect(out_key, out_val);					
		}*/
		
	}

}
