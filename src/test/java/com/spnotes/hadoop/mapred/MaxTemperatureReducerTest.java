package com.spnotes.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class MaxTemperatureReducerTest {

	
	@Test
	public void returnsMaximumIntegerInValues() throws IOException,
	InterruptedException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(10));
		values.add(new IntWritable(5));
		
		ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.withReducer(new MaxTemperatureReducer());
		reduceDriver.withInput(new Text("1950"),values);
		reduceDriver.withOutput(new Text("1950"), new IntWritable(10));
		reduceDriver.runTest();

	}
}
