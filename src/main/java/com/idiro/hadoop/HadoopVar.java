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


import org.apache.log4j.Logger;

import com.idiro.hadoop.utils.HadoopPreferences;

/**
 * Stores and retrieves hadoop administration variable.
 * 
 *  Stores and retrieves: 
 *  zookeeper quorum, 
 *  zookeeper port,
 *  hive uri metastore for hive,
 *  metastore administration uri to create metastores for hbase.
 *  
 * @author etienne
 *
 */
public class HadoopVar {

	public static final String key_s_zkquorum = "zkQuorum";
	public static final String key_s_zkport = "zkPort";
	public static final String key_s_hive = "hiveURI";
	public static final String key_s_adminMetastoreURI = "metastore";
	public static final String key_s_IdiroEngineHDFSPath = "idiroenginehdfspath";
	
	private static Logger logger = Logger.getLogger(HadoopVar.class);
	
	private static String key[] = {key_s_zkquorum,key_s_zkport,key_s_hive,key_s_adminMetastoreURI,key_s_IdiroEngineHDFSPath};


	/**
	 * Preferences node
	 */
	private static HadoopPreferences prefNode= HadoopPreferences.systemNodeForPackage(HadoopVar.class);

	/**
	 * Get the value associate to the key from the preferences tree
	 * @return
	 */
	public static String get(String keyToFound){
		logger.debug("get value for "+prefNode.absolutePath()+" : "+keyToFound);
		boolean found = false;
		int i = 0;
		while(!found && i < key.length ){
			if(!(found = keyToFound.equals(key[i]))) ++i;
		}
		if(!found){
			logger.debug("key not found returns empty value");
			return "";
		}
		logger.debug("key found, value returned: "+prefNode.get(key[i], ""));
		return prefNode.get(key[i], "");
	}

	/**
	 * Put a new value associate to the key from the preferences tree
	 * @return
	 */
	public static void put(String keyToFound,String value){
		logger.debug("set a new value for "+prefNode.absolutePath()+", "+keyToFound);
		boolean found = false;
		int i = 0;
		while(!found && i < key.length ){
			if(!(found = keyToFound.equals(key[i]))) ++i;
		}
		if(!found){
			logger.debug("key not found");
			return;
		}
		logger.debug("key found, replace '"+prefNode.get(key[i], "")+"', by '"+value+"'");
		prefNode.put(key[i], value);
	}

	/**
	 * Reset the value of the log4j properties path in the tree
	 */
	public static void reset(){
		for(int i=0; i < key.length;++i){
			put(key[i],"");
		}
	}
	
	public static String[] getKey() {
		return key;
	}
}
