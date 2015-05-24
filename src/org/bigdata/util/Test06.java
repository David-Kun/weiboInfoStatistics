package org.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.Text;

public class Test06 {
	public static void main(String[] args)throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Path path = new Path("/map.dat");
		Option optKey = MapFile.Writer.keyClass(IntWritable.class);
		org.apache.hadoop.io.SequenceFile.Writer.Option optValue = MapFile.Writer.valueClass(Text.class);
		MapFile.Writer writer = new MapFile.Writer(config, path, optKey, optValue);
		for(int i = 0; i < 100; i++){
			writer.append(new IntWritable(i), new Text("Hello"));
		}
		writer.close();
	}
}
