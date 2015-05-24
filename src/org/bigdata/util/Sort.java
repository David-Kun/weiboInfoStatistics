package org.bigdata.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {
	private static int line = 1;
	
	private static class SortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
				int data = Integer.parseInt(value.toString());
				context.write(new IntWritable(data), new IntWritable(1));
		}
		
	}
	
	private static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

		@Override
		protected void reduce(
				IntWritable key,
				Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
				for(IntWritable value : values){
					context.write(new IntWritable(line), key);
					line++;
				}
		}
		
	}
	
	public static void main(String[] args)throws Exception{
		Configuration config = HadoopConfig.getConfig();
		config.setInt("amount", 3);
		Job job = Job.getInstance(config);
		job.setJobName("排序");
		job.setJarByClass(Sort.class);
		job.setMapperClass(SortMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/SortInput"));
		FileOutputFormat.setOutputPath(job, new Path("/SortOutput/"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
