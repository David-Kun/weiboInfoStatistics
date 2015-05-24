package org.bigdata.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Signal {
	private static class SignalMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				String str = value.toString();
				if(!str.contains("child")){
					String[] strs = str.split(" ");
					context.write(new Text(strs[0]), new Text(strs[1] + ":1"));
					context.write(new Text(strs[1]), new Text(strs[0] + ":2"));
				}
		}
		
	}
	
	private static class SignalReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				List<String> left = new ArrayList<String>();
				List<String> right = new ArrayList<String>();
				for(Text value: values){
					String[] strs = value.toString().split(":");
					if(strs[1].equals("1")){
						right.add(strs[0]);
					} else {
						left.add(strs[0]);
					}
				}
				
				for(String strLeft : left){
					for(String strRight : right){
						context.write(new Text(strLeft), new Text(strRight));
					}
				}
		}
		
	}
	
	public static void main(String[] args)throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config);
		job.setJobName("单表链接");
		job.setJarByClass(Signal.class);
		job.setMapperClass(SignalMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(SignalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/SignalInput"));
		FileOutputFormat.setOutputPath(job, new Path("/SignalOutput/"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
