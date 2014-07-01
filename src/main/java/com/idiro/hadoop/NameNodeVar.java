package com.idiro.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Stores the hadoop namenode URI in memory.
 * 
 * The only indispensable variable for reading what is on HDFS
 * is the namenode URI.
 * We retrieve all the other variables from here.
 * @author etienne
 *
 */
public class NameNodeVar {
	
	private String nameNodeURI;
	
	private static NameNodeVar instance = new NameNodeVar();
	
	private NameNodeVar(){}
	
	public static boolean isInit(){
		return instance.nameNodeURI != null && !instance.nameNodeURI.isEmpty();
	}
	
	public static String get(){
		return instance.nameNodeURI;
	}
	
	public static void set(String nameNodeURI){
		instance.nameNodeURI = nameNodeURI;
	}
	
	/**
	 * Get a basic configuration file with the right fs.default
	 * @return
	 */
	public static Configuration getConf(){
		Configuration conf = new Configuration();
		if(isInit()){
			conf.set("fs.default.name", NameNodeVar.get());
		}
		return conf;
	}
	
	/**
	 * Get the HDFS
	 * @return
	 * @throws IOException
	 */
	public static FileSystem getFS() throws IOException{
		if(isInit()){
			return FileSystem.get(NameNodeVar.getConf());
		}
		return null;
	}
	
}
