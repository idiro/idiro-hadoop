/** 
 *  Copyright © 2016 Red Sqirl, Ltd. All rights reserved.
 *  Red Sqirl, Clarendon House, 34 Clarendon St., Dublin 2. Ireland
 *
 *  This file is part of Idiro Utility for Hadoop
 *
 *  User agrees that use of this software is governed by: 
 *  (1) the applicable user limitations and specified terms and conditions of 
 *      the license agreement which has been entered into with Red Sqirl; and 
 *  (2) the proprietary and restricted rights notices included in this software.
 *  
 *  WARNING: THE PROPRIETARY INFORMATION OF Idiro Utility for Hadoop IS PROTECTED BY IRISH AND 
 *  INTERNATIONAL LAW.  UNAUTHORISED REPRODUCTION, DISTRIBUTION OR ANY PORTION
 *  OF IT, MAY RESULT IN CIVIL AND/OR CRIMINAL PENALTIES.
 *  
 *  If you have received this software in error please contact Red Sqirl at 
 *  support@redsqirl.com
 */

package com.idiro.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Stores the hadoop namenode URI in memory.
 * 
 * The only indispensable variable for reading what is on HDFS is the namenode
 * URI. We retrieve all the other variables from here.
 * 
 * @author etienne
 *
 */
public class NameNodeVar {

	public static final String SERVER_PRINCIPAL_KEY = "user.principal"; 
	public static final String SERVER_KEYTAB_KEY = "user.keytab"; 
	
	private static Logger logger = Logger.getLogger(NameNodeVar.class);
	
	private String jobtracker;

	private String nameNodeURI;

	private Configuration conf;

	private static NameNodeVar instance = new NameNodeVar();

	private NameNodeVar() {
	}

	public static boolean isInit() {
		return instance.nameNodeURI != null && !instance.nameNodeURI.isEmpty();
	}

	public static boolean isTrackerInit() {
		return instance.jobtracker != null && !instance.jobtracker.isEmpty();
	}

	public static String get() {
		return instance.nameNodeURI;
	}

	public static void setJobTracker(String jobtracker) {
		instance.jobtracker = jobtracker;
	}

	public static String getJobTracker() {
		return instance.jobtracker;
	}

	public static void set(String nameNodeURI) {
		instance.nameNodeURI = nameNodeURI;
	}

	/**
	 * Get a basic configuration file with the right fs.default
	 * 
	 * @return
	 */
	public static Configuration getConf() {
		if(instance.conf == null){
			initConf();
		}
		return new Configuration(instance.conf);
	}

	/**
	 * list files in the given directory and subdirs (with recursion)
	 * @param paths
	 * @return
	 */
	public static List<File> getFiles(String paths) {
		List<File> filesList = new ArrayList<File>();
		for (final String path : paths.split(File.pathSeparator)) {
			final File file = new File(path);
			if( file.isDirectory()) {
				recurse(filesList, file);
			}
			else {
				filesList.add(file);
			}
		}
		return filesList;
	}

	private static void recurse(List<File> filesList, File f) { 
		File list[] = f.listFiles();
		for (File file : list) {
			if (file.isDirectory()) {
				recurse(filesList, file);
			}
			else {
				filesList.add(file);
			}
		}
	}

	/**
	 * Get the HDFS
	 * 
	 * @return
	 * @throws IOException
	 */
	public static FileSystem getFS() throws IOException {
		if (isInit()) {
			return FileSystem.get(getConf());
		}
		return null;
	}

	public static int getNbSlaves() {
		int slaves = 0;
		if (isTrackerInit()) {
			try {
				JobClient theJobClient = new JobClient(new InetSocketAddress(
						instance.getJobTracker().substring(0,
								instance.getJobTracker().indexOf(":")),
								Integer.valueOf(
										instance.getJobTracker()
										.substring(
												instance.getJobTracker()
												.indexOf(":") + 1))
												.intValue()), instance.getConf());

				slaves = theJobClient.getClusterStatus().getTaskTrackers();
				
			} catch (IOException e) {
				System.out.println(e);
			}

		}

		return slaves;
	}
	
	protected static void initConf(){
		if (isInit()) {
			Configuration conf = new Configuration();
			List<File> list = getFiles(System.getProperty("java.class.path"));
			conf.set("fs.default.name", NameNodeVar.get());
			for (File file: list) {
				if(file.getPath().contains("hadoop-client")){ //hadoop version 2.X
					conf.set("fs.defaultFS", NameNodeVar.get());
					break;
				}
			}
			instance.conf = conf;
		}
	}
	
	public static void addToDefaultConf(String name, String value){
		if(instance.conf == null){
			initConf();
		}
		if(instance.conf != null){
			instance.conf.set(name, value);
		}
	}
	
	public static void removeFromDefaultConf(String name){
		if(instance.conf == null){
			initConf();
		}
		if(instance.conf != null){
			instance.conf.unset(name);
		}
	}
	
	public static String getConfStr(){
		return getConfStr(getConf());
	}
	
	public static String getConfStr(Configuration conf){
		String ans = "";
		Map<String, String> params = conf.getValByRegex(".*");
		Iterator<Entry<String,String>> it = params.entrySet().iterator();
		while(it.hasNext()){
			Entry<String,String> cur = it.next();
			ans += cur.getKey()+":"+cur.getValue()+"\n";
		}
		return ans;
	}
}