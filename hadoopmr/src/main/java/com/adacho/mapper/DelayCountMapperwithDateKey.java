package com.adacho.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.adacho.common.AirlinePerformanceParser;
import com.adacho.common.DateKey;
import com.adacho.counter.DelayCounters;

public class DelayCountMapperwithDateKey extends Mapper<LongWritable, Text, DateKey, IntWritable>{
	
	private final static IntWritable outputVale = new IntWritable(1);
	private DateKey outputKey = new DateKey();
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DateKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		
		if (parser.isDepartureDelayAvailalbe()) {
			if (parser.getDepartureDelayTime() > 0) {
				outputKey.setYear("D," + parser.getYear());
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, outputVale);
			} else if (parser.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			} else if (parser.getDepartureDelayTime() < 0) {
				context.getCounter(DelayCounters.early_departure).increment(1);

			}
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}

		if (parser.isArriveDelayAvailalbe()) {
			if (parser.getArriveDelayTime() > 0) {
				outputKey.setYear("A," + parser.getYear());
				outputKey.setMonth(parser.getMonth());
				context.write(outputKey, outputVale);
			} else if (parser.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			} else if (parser.getArriveDelayTime() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);

			}
		} else {
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
		}
		
	}
	
	
	
	
	
	
	
	

}
