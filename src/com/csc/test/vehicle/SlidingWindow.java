package com.csc.test.vehicle;

import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.csc.test.vehicle.parsers.AccelParser;

public class SlidingWindow {

	
	static Log log = LogFactory.getLog(VehicleTestMapper.class);
	
	LinkedList<AccelParser> oCurrentWindow; // = new LinkedList<Integer>();
	
	long _lWindowSize;
	long _lSlideIncrement;
	long _lCurrentTime;
	long _lSampleSize;
	
	public SlidingWindow( long WindowSizeInMS, long SlideIncrement, long sample_size ) {
	
		this._lWindowSize = WindowSizeInMS;
		this._lSlideIncrement = SlideIncrement;
		this._lCurrentTime = 0;
		this._lSampleSize = sample_size;
		
		this.oCurrentWindow = new LinkedList<AccelParser>();
		
	}
	
	public long GetWindowStepSize() {
		
		return this._lSlideIncrement;
		
	}
	
	public long GetWindowSize() {
		return this._lWindowSize;
	}
	
	
	public boolean WindowIsFull() {
		
		log.info("WindowDelta: "+this.GetWindowDelta()+" && WindowSize: "+this._lWindowSize);
		System.out.println("WindowDelta: "+this.GetWindowDelta()+" && WindowSize: "+this._lWindowSize);
		
		if ( this.GetWindowDelta() >= this._lWindowSize ) {
			return true;
		}
		
		return false;
		
	}
	
	public long GetWindowDelta() {
		
		if ( this.oCurrentWindow.size() > 0 ) {
			return this.oCurrentWindow.getLast().getEventTime() - this.oCurrentWindow.getFirst().getEventTime() + this._lSampleSize;
		}
		
		return 0;
		
	}
	
	public void AddPoint( AccelParser point ) throws Exception {

		// look at back of window
		
		// if back of window is greater than this point, throw exception
		if ( this.oCurrentWindow.size() > 0) {
			if ( point.getEventTime() <= this.oCurrentWindow.getLast().getEventTime() ) {
				throw new Exception( "Point out of order!" );
			}
		}
		
		this.oCurrentWindow.add( point );
		
	}
	
	public int GetNumberPointsInWindow() {
		
		return this.oCurrentWindow.size();
		
	}

	/**
	 * Slide the window forward
	 * - burn off the first half of the window
	 * - still must re-add more points from the Reduce iterator
	 * @throws Exception
	 */
	public void SlideWindowForward() {
		
		long lCurrentFrontTS = this.oCurrentWindow.getFirst().getEventTime(); //.GetCalendar().getTimeInMillis();
		this._lCurrentTime = lCurrentFrontTS + this._lSlideIncrement;
		
		// now burn off the tail
		
		while ( this.oCurrentWindow.getFirst().getEventTime() < this._lCurrentTime ) {

			this.oCurrentWindow.removeFirst();
	
		}				
		
	}	


	public LinkedList<AccelParser> GetCurrentWindow() {
		
		return this.oCurrentWindow;
		
	}

	
}

