package org.bigdata.pageRank;

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
import org.bigdata.util.HadoopConfig;

public class ProbabilityVector {
	private static class ProbabilityVectorMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] strs = str.split("\t");
			String user = strs[0].substring(1, strs[0].length()-1);
			context.write(new Text(user), NullWritable.get());
		}
		
	}
	
	private static class ProbabilityVectorReducer extends Reducer<Text, NullWritable, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text("a	"+"63641"));
		}
		
	}
	
	public static void runProbabilityVector()throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"PageRank First");
		job.setJarByClass(ProbabilityVector.class);
		job.setMapperClass(ProbabilityVectorMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass(ProbabilityVectorReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/ProbabilityVectorInput"));
		FileOutputFormat.setOutputPath(job,new Path("/ProbabilityVectorOutput/"));
		job.waitForCompletion(true);
	}
}



