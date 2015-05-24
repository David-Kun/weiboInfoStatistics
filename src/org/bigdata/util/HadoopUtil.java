package org.bigdata.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

import com.jcraft.jsch.Compression;


/**
 * Hadoop ������
 * @author David
 *
 */
public class HadoopUtil {
	
	/**
	 * ����Ŀ¼
	 * @param dirPath
	 * @throws IOException
	 */
	public static void mkdir(String dirPath) throws IOException{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.mkdirs(new Path(dirPath));
		fs.close();
	}
	
	/**
	 * �����ļ�
	 * @param filePath
	 * @throws Exception
	 */
	public static void createFile(String filePath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.create(new Path(filePath));
		fs.close();
	}
	
	/**
	 * ɾ���ļ�
	 * @param filePath
	 * @throws Exception
	 */
	public static void deleteFile(String filePath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.deleteOnExit(new Path(filePath));
		fs.close();
	}
	/**
	 * �����ļ�
	 * @param filePath
	 * @throws Exception
	 */
	public static void listFilfe(String filePath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		FileStatus[] status = fs.listStatus(new Path(filePath));
		for(FileStatus s : status){
			System.out.println(s.getPath().toString());
		}
		fs.close();
	}
	
	/**
	 * �ϴ��ļ�
	 * @param src
	 * @param dest
	 * @throws Exception
	 */
	public static void upLoad(String src, String dest) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		fs.close();
	}
	
	/**
	 * �����ļ�
	 * @param src
	 * @param dest
	 * @throws Exception
	 */
	public static void downLoad(String src, String dest) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.copyToLocalFile(new Path(src), new Path(dest));
		fs.close();
	}
	
	
	/**
	 * ��һ���ļ�ѹ����hadoop
	 * @param args
	 * @throws Exception
	 */
//	public static void main(String[] args) throws Exception{
//		Configuration config = HadoopConfig.getConfig();
//		Path path = new Path("/hello.gz");
//		FileSystem fs = FileSystem.get(config);
//		OutputStream os = fs.create(path);
//		CompressionCodec codec = new GzipCodec();
//		CompressionOutputStream cos =  codec.createOutputStream(os);
//		cos.write("hello world".getBytes());
//		cos.close();
//	}
	/**
	 * ��ѹ�ļ�
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		Path path = new Path("/hello.gz");
		FileSystem fs = FileSystem.get(config);
		InputStream is = fs.open(path);
		GzipCodec codec = new GzipCodec();
		codec.setConf(config);
		CompressionInputStream cis =  codec.createInputStream(is);
		byte[] buffer = new byte[1024];
		int read = -1;
		while( (read = cis.read(buffer)) != -1){
			System.out.println(new String(buffer, 0, read));
		}
		cis.close();
	}
	
//	public static void main(String[] args) throws Exception{
////		mkdir("/hadoop");
////		createFile("/hadoop/mytest.java");
////		deleteFile("/hadoop/mytest.java");
////		downLoad("/hadoop/mytest.java","D:\\eclipseWorkplace/myTest/mytest.java");
//	}
	
}
