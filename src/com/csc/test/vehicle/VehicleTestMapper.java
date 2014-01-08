package com.csc.test.vehicle;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.csc.test.vehicle.parsers.AccelParser;

public class VehicleTestMapper extends MapReduceBase implements Mapper<LongWritable, Text, TimeseriesKey, AccelParser> {
	
	static Log log = LogFactory.getLog(VehicleTestMapper.class);
			
	static enum PointCounters {
		POINTS_SEEN, POINTS_ADDED_TO_WINDOWS, MOVING_AVERAGES_CALCD
	};
	
	//private static final String DATE_FORMAT = "M/d/yyyy";
	
	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"; //yyyy-MM-dd HH:mm:ss:SSS

	private static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
	
	private final TimeseriesKey mapKey = new TimeseriesKey();
	private final AccelParser mapVal = new AccelParser();

	@Override
	public void map(LongWritable key, Text values, OutputCollector<TimeseriesKey, AccelParser> output,
			Reporter reporter) throws IOException {
		FileSplit split = (FileSplit)reporter.getInputSplit();
		String fileName = split.getPath().getName();
			
		String tripId, eventTime, accelData1, accelData2, accelData3;
					
		log.info("String read from file: "+values.toString());
		StringTokenizer token = new StringTokenizer(values.toString(), ",");
		while(token.hasMoreTokens()){
			
			reporter.incrCounter(PointCounters.POINTS_SEEN, 1);
			
			tripId = token.nextToken();
			eventTime = token.nextToken();
			accelData1 = token.nextToken();
			accelData2 = token.nextToken();
			accelData3 = token.nextToken();
			
			
			//mapVal.setTripId(tripId);
			try {				
				mapKey.set(tripId, sdf.parse(eventTime).getTime());
				mapVal.setEventTime(sdf.parse(eventTime).getTime());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			mapVal.setAccelData1Min(accelData1);
			mapVal.setAccelData1Max(accelData1);
			mapVal.setAccelData2Min(accelData2);
			mapVal.setAccelData2Max(accelData2);
			mapVal.setAccelData3Min(accelData3);
			mapVal.setAccelData3Max(accelData3);
			
			
			output.collect(mapKey, mapVal);
		}
		
		/*int firstCommaIndex = values.toString().indexOf(",");
		String mapKey = values.toString().substring(0, firstCommaIndex);
		String mapvalue = values.toString().substring(firstCommaIndex, values.toString().length());
		
		txtMapKey.set(mapKey);
		txtMapVal.set(mapvalue);*/
		
		//output.collect(txtMapKey, parser);			
			
			
		/*Deserializer deserializer = CsvIOFactory.createFactory(AcceleratorParser.class).createDeserializer();
        deserializer.open(createFileReader(fileName));
        while (deserializer.hasNext()) {
            AcceleratorParser accelParser = deserializer.next();
        }
        deserializer.close(true);*/

		}
		
	}

