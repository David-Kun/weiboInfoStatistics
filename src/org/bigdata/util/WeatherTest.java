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


public class WeatherTest {
	private static class WeatherMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
				String str = value.toString();
				String year = str.substring(0, 4);
				String temp = str.substring(14, 19).trim();
				if(!"-9999".equals(temp)){
					context.write(new Text(year), new IntWritable(Integer.parseInt(temp)));
				}
		}
	}
	
	private static class WeatherCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
				int max = Integer.MIN_VALUE;
				for(IntWritable value : values){
					if(value.get() > max) max = value.get();
				}
				context.write(key, new IntWritable(max));
		}
		
	}
	
	private static class WeatherReducer extends  Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
				int max = Integer.MIN_VALUE;
				for(IntWritable value : values){
					if(value.get() > max) max = value.get();
				}
				context.write(key, new IntWritable(max));
		}
		
	}
	
	public static void main(String[] args)throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config);
		job.setJobName("Weather Count");
		job.setJarByClass(WeatherTest.class);
		job.setMapperClass(WeatherMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(WeatherCombiner.class);
		
		job.setReducerClass(WeatherReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/input"));
		FileOutputFormat.setOutputPath(job, new Path("/output/"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}





