package com.spnotes.hadoop.mapred;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MaxTemperatureDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
	
		Job job = new org.apache.hadoop.mapreduce.Job();
	//	job.getConfiguration().addResource("hadoop-local.xml");
		
		System.out.println(job.getConfiguration().get("fs.default.name"));
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setJobName("Max Temperature");

		System.out.println("Input path " + args[0]);
		System.out.println("Oupput path " + args[1]);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}

	public static void main(String[] argv) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(), argv);
		System.exit(exitCode);
	}
}
