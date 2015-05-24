package org.bigdata.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Dup {
	private static class DupMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
				context.write(value, NullWritable.get());
		}
		
	}
	
	private static class DupReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
				context.write(key, NullWritable.get());
		}
		
	}
	
	public static void main(String[] args)throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config);
		job.setJobName("去重");
		job.setJarByClass(Dup.class);
		job.setMapperClass(DupMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(DupReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path("/DupInput"));
		FileOutputFormat.setOutputPath(job, new Path("/DupOutput/"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
