package com.adacho.common;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {
	private int year;
	private int month;
	private int day;
	
	private int arriveDelayTime = 0;
	private int departureDelayTime = 0;
	private int distance = 0;
	
	private boolean arriveDelayAvailalbe = true;
	private boolean departureDelayAvailalbe = true;
	private boolean distanceAvailalbe = true;
	
	private String uniqueCarrier;
	
	public AirlinePerformanceParser(Text text) {
		try {
			String[] columns = text.toString().split(",");
			
			year = Integer.parseInt(columns[0]);
			month = Integer.parseInt(columns[1]);
			day = Integer.parseInt(columns[2]);
			uniqueCarrier  = columns[5];
			
			if(!columns[16].equals("")) {
				departureDelayTime = (int)Float.parseFloat(columns[16]);
			}
			else {
				departureDelayAvailalbe = false;
			}
			
			if(!columns[26].equals("")) {
				arriveDelayTime = (int)Float.parseFloat(columns[26]);
			}
			else {
				arriveDelayAvailalbe = false;
			}
			
			if(!columns[37].equals("")) {
				distance = (int)Float.parseFloat(columns[37]);
			}
			else {
				distanceAvailalbe = false;
			}
			
			
			
			
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		
		
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getArriveDelayTime() {
		return arriveDelayTime;
	}

	public void setArriveDelayTime(int arriveDelayTime) {
		this.arriveDelayTime = arriveDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public void setDepartureDelayTime(int departureDelayTime) {
		this.departureDelayTime = departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public boolean isArriveDelayAvailalbe() {
		return arriveDelayAvailalbe;
	}

	public void setArriveDelayAvailalbe(boolean arriveDelayAvailalbe) {
		this.arriveDelayAvailalbe = arriveDelayAvailalbe;
	}

	public boolean isDepartureDelayAvailalbe() {
		return departureDelayAvailalbe;
	}

	public void setDepartureDelayAvailalbe(boolean departureDelayAvailalbe) {
		this.departureDelayAvailalbe = departureDelayAvailalbe;
	}

	public boolean isDistanceAvailalbe() {
		return distanceAvailalbe;
	}

	public void setDistanceAvailalbe(boolean distanceAvailalbe) {
		this.distanceAvailalbe = distanceAvailalbe;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}
	
	

}
