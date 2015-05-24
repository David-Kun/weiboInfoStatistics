package org.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;

public class Test04 {
	/**
	 * ��С�ļ����л���ȫ�ŵ�һ���ļ��У�
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Path path = new Path("/seq.dat");
		Option optPath = SequenceFile.Writer.file(path);
		Option optKey = SequenceFile.Writer.keyClass(IntWritable.class);
		Option optValue = SequenceFile.Writer.valueClass(Text.class);
		Writer writer = SequenceFile.createWriter(config, optPath, optKey, optValue);
		for(int i = 0; i < 100; i++){
			writer.append(new IntWritable(i), new Text("Hello"));
		}
		writer.close();
	}
}
