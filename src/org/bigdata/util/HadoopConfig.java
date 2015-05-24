package org.bigdata.util;

import org.apache.hadoop.conf.Configuration;

/**
 * Hadoopµƒ≈‰÷√–≈œ¢
 * @author David
 *
 */

public class HadoopConfig {
	
	private static Configuration config;
	
	private HadoopConfig(){}
	
	public static Configuration getConfig(){
		if (config == null){
			config = new Configuration();
			config.addResource(HadoopConfig.class.getResource("core-site.xml"));
			config.addResource(HadoopConfig.class.getResource("hdfs-site.xml"));
			config.addResource(HadoopConfig.class.getResource("yarn-site.xml"));
		}
		return config;
	}
}
