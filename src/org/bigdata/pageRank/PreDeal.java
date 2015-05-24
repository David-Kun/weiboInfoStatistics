package org.bigdata.pageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bigdata.util.HadoopConfig;

/**
 * 微博数据预处理
 * @author David
 *用户的关注关系
 */
public class PreDeal {
	private static class PreDealMapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] strs = str.split("\t");
			String suer = strs[0].substring(1, strs[0].length()-1);
			String tuer = strs[1].substring(1, strs[1].length()-1);
			context.write(new Text(suer), new Text(tuer));
		}
		
	}
	private static class PreDealReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for(Text value : values){
				sb.append(value.toString()).append("\t");
			}
			context.write(key, new Text(sb.toString()));
		}
		
	}
	
	public static void runPreDeal()throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Job job = Job.getInstance(config,"PageRank First");
		job.setJarByClass(PreDeal.class);
		job.setMapperClass(PreDealMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(PreDealReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/PreDealInput"));
		FileOutputFormat.setOutputPath(job,new Path("/PreDealOutput/"));
		job.waitForCompletion(true);
	}
}
