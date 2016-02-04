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

import com.idiro.utils.db.JdbcPrefsDetails;

/**
 * Stores/Retrieves jdbc details in hdfs.
 * 
 * 
 * @author etienne
 *
 */
public class JdbcHdfsPrefsDetails extends JdbcPrefsDetails{

	/**
	 * pref pointing on the node where username, password and location are stored
	 */
	protected HadoopPreferences prefs = HadoopPreferences.userNodeForPackage(getClass());
	
	public JdbcHdfsPrefsDetails(String dburl) throws Exception{
		super(dburl);
		//Make sure details exist
		//if(prefs.get(userKey, "").isEmpty())
		//	throw new Exception("The database url is not link to any database details");
	}
	
	public JdbcHdfsPrefsDetails(String dburl,String username,String password){
		super(dburl);
		setUsername(username);
		setPassword(password);
	}
	
	
	public void remove(){
		prefs.remove(passwdKey);
		prefs.remove(userKey);
	}
	
	/**
	 * @return the dburl
	 */
	public String getDburl() {
		return dburl;
	}

	/**
	 * @param dburl the dburl to set
	 */
	public void setDburl(String dburl) {
		String password = getPassword();
		String username = getUsername();
		prefs.remove(passwdKey);
		prefs.remove(userKey);
		this.dburl = dburl;
		resetKeys();
		prefs.put(passwdKey, password);
		prefs.put(userKey, username);
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return prefs.get(userKey, "");
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		prefs.put(userKey, username);
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return prefs.get(passwdKey, "");
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		prefs.put(passwdKey, password);
	}

	
}
