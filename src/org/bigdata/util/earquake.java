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

public class earquake {
	
	private static String[] CNProvinces = { "新疆", "甘肃", "云南", "江苏", "四川", "广西",
		"河北", "山东", "山西", "贵州", "内蒙古", "西藏", "青海", "辽宁", "陕西", "黑龙江", "北京",
		"天津", "上海", "重庆", "河南", "湖南", "安徽", "浙江", "江西", "湖北", "吉林", "福建",
		"广东", "海南", "台湾", "香港", "澳门", "其他" };
	
	private static String isIn(String name){
		for(int i = 0; i < CNProvinces.length; i++){
			if(name.indexOf(CNProvinces[i]) != -1){
				return CNProvinces[i];
			}
		}
		return "others";
	}

	private static class EarthQuakeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
				String[] strs = value.toString().split(",");
				if (strs.length < 9) return;
				String location = strs[8];
				String tmp = isIn(location);
				if(tmp.equals("others")) tmp = "其他";
				context.write(new Text(tmp), new IntWritable(1));
		}
	}
	
	private static class EarthQuakeReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

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
		job.setJobName("统计地震数据");
		job.setJarByClass(earquake.class);
		job.setMapperClass(EarthQuakeMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(EarthQuakeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/earthquakeInput"));
		FileOutputFormat.setOutputPath(job, new Path("/earthquakeOutput/"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}



