package com.adacho.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.adacho.mapper.ArrivalDelayCountMapper;
import com.adacho.reducer.ArrivalDelayCountReducer;

public class ArrivalDelayCount {
	
	public static void main(String[] args) throws Exception{
	
	Configuration conf = new Configuration();
	if(args.length !=2) {
		System.out.println("Usage: WordCount <input> <output>");
		System.exit(2);
		System.out.println("MAN!!");
	}
	
	Job job = Job.getInstance(conf, "WordCount");
	
	job.setJarByClass(ArrivalDelayCount.class);
	job.setMapperClass(ArrivalDelayCountMapper.class);
	job.setReducerClass(ArrivalDelayCountReducer.class);
	
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	
	job.setOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	job.waitForCompletion(true);


}
}
