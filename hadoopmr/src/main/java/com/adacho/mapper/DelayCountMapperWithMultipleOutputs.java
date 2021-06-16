package com.adacho.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.adacho.common.AirlinePerformanceParser;
import com.adacho.counter.DelayCounters;

public class DelayCountMapperWithMultipleOutputs extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable outputVale = new IntWritable(1);
	private Text outputKey = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

		if (parser.isDepartureDelayAvailalbe()) {
			if (parser.getDepartureDelayTime() > 0) {
				outputKey.set("D," + parser.getYear() + "," + parser.getMonth()); // "2012,01, 2012,02"
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
				outputKey.set("A," + parser.getYear() + "," + parser.getMonth()); // "2012,01, 2012,02"
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
