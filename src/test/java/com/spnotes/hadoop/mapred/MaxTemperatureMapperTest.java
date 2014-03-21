package com.spnotes.hadoop.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class MaxTemperatureMapperTest {

	@Test
	public void processValidRecord() throws Exception {
		Text value = new Text(
				"0043011990999991950051518004+68750+023550FM-12+0382" + // Year
																		// ^^^^
						"99999V0203201N00261220001CN9999999N9-00111+99999999999"); // Temperature
		/*
		 * // ^^^^^ new MapDriver<LongWritable, Text, Text, IntWritable>()
		 * .withMapper(new MaxTemperatureMapper()).withInputValue(value)
		 * .withOutput(new Text("1950"), new IntWritable(-11)).runTest();
		 */

		Pair<LongWritable, Text> input = new Pair<LongWritable, Text>(
				new LongWritable(1), value);
		Pair<Text, IntWritable> output = new Pair<Text, IntWritable>(new Text(
				"1950"), new IntWritable(-11));
		new MapDriver<LongWritable, Text, Text, IntWritable>()
				.withMapper(new MaxTemperatureMapper()).withInput(input)
				.withOutput(output).runTest();
	}

	@Test
	public void ignoresMissingTemperatureRecord() throws Exception,
	InterruptedException {
	Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
	 "99999V0203201N00261220001CN9999999N9+99991+99999999999");
	// Temperature ^^^^^
	

	Pair<LongWritable, Text> input = new Pair<LongWritable, Text>(
			new LongWritable(1), value);
	new MapDriver<LongWritable, Text, Text, IntWritable>()
	.withMapper(new MaxTemperatureMapper()) .withInput(input)
	.runTest();
	}
}
