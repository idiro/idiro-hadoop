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

package com.idiro.hadoop.utils;


import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.idiro.hadoop.NameNodeVar;

/**
 * Utility methods for creating temporary files.
 * 
 * Creates temporary paths in a specific folder on hdfs.
 * At the moment of writing the folder is "/tmp/idiro/"
 * 
 * @author etienne
 *
 */
public class TmpPath {

	private static final Logger logger = Logger.getLogger(TmpPath.class);
	public static final String tmpPath = "/tmp/idiro/",
			tmpUserPath = "/user/"+System.getProperty("user.name")+"/tmp/";
	public static final int length = 15;


	/**
	 * Create a temporary path that does not exists.
	 * 
	 * Create a temporary path that does not exists
	 * and with a random name
	 * @return
	 * @throws Exception
	 */
	public static Path create() throws Exception{
		return create("","");
	}

	/**
	 * Create a temporary path that does not exists.
	 * 
	 * Create a temporary path that does not exists
	 * and with a random name with a suffix.
	 * 
	 * @param suffix
	 * @return
	 * @throws Exception
	 */
	public static Path create(String suffix) throws Exception{
		return create("",suffix);
	}
	
	/**
	 * Create a temporary path in user home folder that does not exists.
	 * 
	 * Create a temporary path that does not exists
	 * and with a random name
	 * @return
	 * @throws Exception
	 */
	public static Path create_user() throws Exception{
		return create_user("","");
	}

	/**
	 * Create a temporary path in user home folder that does not exists.
	 * 
	 * Create a temporary path that does not exists
	 * and with a random name with a suffix.
	 * 
	 * @param suffix
	 * @return
	 * @throws Exception
	 */
	public static Path create_user(String suffix) throws Exception{
		return create_user("",suffix);
	}
	
	

	/**
	 * Create a temporary path that does not exists.
	 * 
	 * Create a temporary path that does not exists
	 * and with a random name with a prefix and a suffix.
	 * @param prefix
	 * @param suffix
	 * @return
	 * @throws Exception
	 */
	public static Path create(String prefix, String suffix) throws Exception{
		return create(true,prefix,suffix);
	}
	
	/**
	 * Create a temporary path that does not exists.
	 * 
	 * Create a temporary path that does not exists
	 * and with a random name with a prefix and a suffix.
	 * @param prefix
	 * @param suffix
	 * @return
	 * @throws Exception
	 */
	public static Path create_user(String prefix, String suffix) throws Exception{
		return create(false,prefix,suffix);
	}
	
	
	private static Path create(boolean sys, String prefix, String suffix) throws Exception{
		StringBuilder strb = null;
		if(sys){
			strb = new StringBuilder(tmpPath);
		}else{
			strb = new StringBuilder(tmpUserPath);
		}
		
		if(prefix != null && !prefix.isEmpty()){
			strb.append(prefix).append('_');
		}

		String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		Random rnd = new Random();
		char[] new_name = new char[length];
		new_name[0] =  characters.charAt(rnd.nextInt(characters.length()-10));
		for (int i = 1; i < length; i++){
			new_name[i] =  characters.charAt(rnd.nextInt(characters.length()));
		}
		strb.append(new_name);
		if(suffix != null && !suffix.isEmpty()){
			strb.append('_').append(suffix);
		}

		Path ans = new Path(strb.toString());
		FileSystem dfs = NameNodeVar.getFS();
		if(dfs.exists(ans)){
			dfs.close();
			logger.warn("Path "+strb+" already exists, try again");
			ans = create(suffix);
		}else{
			logger.info("New tmp folder created: "+strb.toString());
		}
		return ans;
	}

}
