package com.msg.mq.rocketmq.utils;


import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RocketMqSystemEnv {
	
	private static Map<String, String> propMap = new HashMap<>();

	static {
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("conf/rocketmq.properties");
		Properties prop = new Properties();
		try {
			prop.load(is);
			@SuppressWarnings("rawtypes")
			Enumeration enu = prop.propertyNames();
			while(enu.hasMoreElements()){
				String key = enu.nextElement().toString();
				propMap.put(key, prop.getProperty(key));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static String getPropertie(String key) {
		return propMap.get(key);
	}
}
