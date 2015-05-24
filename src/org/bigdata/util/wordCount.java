package org.bigdata.util;

import java.io.IOException;

import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordCount {
	/**
	 * 将单词分割出来
	 * @author David
	 *
	 */
	private static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
				String[] strs = value.toString().split(" ");
				for(String str : strs){
					context.write(new Text(str), new IntWritable(1));
				}
		}
	}
	
	/**
	 * 统计分割出的单词数（聚集）
	 * @author David
	 *
	 */
	private static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
				int count = 0;
				for(IntWritable value : values){
					count += value.get();
				}
				context.write(key, new IntWritable(count));
		}
		
	}
	
	public static void main(String[] args)throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config);
		job.setJobName("Word Count");
		job.setJarByClass(wordCount.class);
		job.setMapperClass(WordMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(WordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/input"));
		FileOutputFormat.setOutputPath(job, new Path("/output/"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
