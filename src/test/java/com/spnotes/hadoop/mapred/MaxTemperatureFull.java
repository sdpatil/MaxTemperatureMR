package com.spnotes.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class MaxTemperatureFull {
	
	 MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	  
	  @Before
	  public void setUp() {
	    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
	    MaxTemperatureReducer reducer = new MaxTemperatureReducer();
	    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
	    mapDriver.setMapper(mapper);
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
	    reduceDriver.setReducer(reducer);
	    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
	  }

	  @Test
	  public void testMapReduce() throws Exception{
		  List<Pair<LongWritable, Text>> inputList = new ArrayList<Pair<LongWritable,Text>>();
		  inputList.add( new Pair(  new LongWritable(1), new Text(
					"0067011990999991950051507004+68750+023550FM-12+038299999V0203301N00671220001CN9999999N9+00001+99999999999")));
		  inputList.add( new Pair(  new LongWritable(2), new Text(
					"0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+00221+99999999999")));
		  inputList.add( new Pair(  new LongWritable(3), new Text(
					"0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999")));
		  inputList.add( new Pair(  new LongWritable(4), new Text(
					"0043012650999991949032412004+62300+010750FM-12+048599999V0202701N00461220001CN0500001N9+01111+99999999999")));
		  inputList.add( new Pair(  new LongWritable(4), new Text(
					"0043012650999991949032418004+62300+010750FM-12+048599999V0202701N00461220001CN0500001N9+00781+99999999999")));
		  mapReduceDriver.withAll(inputList);
		//  mapReduceDriver.withInput(new LongWritable(1),value);

		  List<Pair<Text, IntWritable>> outputList = new ArrayList<Pair<Text,IntWritable>>();
		  
		  outputList.add(new Pair<Text, IntWritable>(new Text("1949"), new IntWritable(111)));
		  outputList.add(new Pair<Text, IntWritable>(new Text("1950"), new IntWritable(22)));
 		  
		  mapReduceDriver.addAllOutput(outputList);
		  mapReduceDriver.runTest();
	  }
}
