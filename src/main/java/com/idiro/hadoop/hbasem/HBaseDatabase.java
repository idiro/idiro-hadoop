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

package com.idiro.hadoop.hbasem;


import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.idiro.hadoop.HadoopVar;
import com.idiro.hadoop.utils.HadoopPreferences;
import com.idiro.hadoop.utils.JdbcHdfsPrefsDetails;
import com.idiro.utils.db.JdbcConnection;
import com.idiro.utils.db.mysql.MySqlBasicStatement;
import com.idiro.utils.db.mysql.MySqlUtils;


/**
 * 
 * Store, Retrieve and initialise a database for storing hbase metadata.
 * 
 * 
 * @author etienne
 *
 */
public class HBaseDatabase {

	private Logger logger = Logger.getLogger(getClass());

	/**
	 * Preferences node
	 */
	private HadoopPreferences prefNode= HadoopPreferences.userNodeForPackage(HBaseDatabase.class);
	
	public final static String dictionary_table = "hbase_dictionary";
	
	public final static String category_table = "hbase_category";
	
	public final static String dictionary_f_name = "name";
	public final static String dictionary_f_hbid = "hbase_id";
	public final static String dictionary_f_type = "type";
	public final static String dictionary_f_category = "category";
	public final static String dictionary_f_comment = "comment";
	
	public final static String category_f_cat = "category";
	public final static String category_f_parent = "parent_category";
	
	public final static String table_f_hbid = "hbase_id";
	public final static String table_f_colfam = "col_fam";


	/**
	 * Create a database for managing the hbase tables of "name"
	 * @param user
	 * @return true if the database has been created
	 */
	public boolean createDatabase(String name, String user, String user_password){
		boolean ok = true;
		String metastoreUrl = HadoopVar.get(HadoopVar.key_s_adminMetastoreURI);
		String database = "hbaseM_"+name;
		String url = null;//metastoreUrl+"/"+database;
		if(metastoreUrl.lastIndexOf('/') == metastoreUrl.length() -1){
			url = metastoreUrl.substring(
					0, metastoreUrl.substring(0, metastoreUrl.length() -1).lastIndexOf('/'));
		}else{
			url = metastoreUrl.substring(0,metastoreUrl.lastIndexOf('/'));
		}
		url +="/"+database;
		logger.debug("The database url to create is '"+url+"'");
		
		MySqlBasicStatement bs = new MySqlBasicStatement();
		
		if(metastoreUrl.isEmpty()){
			logger.error("No admin database set");
			ok = false;
		}

		if(ok){
			
			//Create the database
			try {
				ok = MySqlUtils.createDatabase(new JdbcHdfsPrefsDetails(metastoreUrl), database,user,user_password);
			} catch (Exception e) {
				logger.error("Cannot get the detail of the database "+metastoreUrl);
				logger.error(e.getMessage());
				ok = false;
			}
			
		}

		//Register this database as hbase manager
		if(ok){
			logger.debug("Link "+name+" to "+url);
			new JdbcHdfsPrefsDetails(url,user,user_password);
			ok = linkDatabase(name,url);
		}

		//Create the default tables
		if(ok){
			logger.debug("Create the default tables");
			try {
				logger.debug("Connect with the new user account...");
				JdbcConnection new_database = new JdbcConnection(new JdbcHdfsPrefsDetails(url),bs);
				
				//Create the table dictionary
				logger.debug("Table dictionary...");
				Map<String,String> features_dic = new LinkedHashMap<String,String>();
				features_dic.put(dictionary_f_name,"VARCHAR(30) BINARY UNIQUE NOT NULL");
				features_dic.put(dictionary_f_hbid, "VARCHAR(15) BINARY NOT NULL");
				features_dic.put(dictionary_f_type, "VARCHAR(15) NOT NULL");
				features_dic.put(dictionary_f_category, "VARCHAR(15) NOT NULL");
				features_dic.put(dictionary_f_comment, "VARCHAR(400)");
				String[] options = {dictionary_f_hbid};
				new_database.createTable(dictionary_table, features_dic, options);
				
				//Create the table category
				logger.debug("Table category...");
				Map<String,String> features_cat = new LinkedHashMap<String,String>();
				features_cat.put(category_f_cat, "VARCHAR(15) NOT NULL");
				features_cat.put(category_f_parent, "VARCHAR(15)");
				options[0] = category_f_cat;
				
				new_database.createTable(category_table, features_cat, options);
				
				(new HBaseManager(name)).addCategory("unknown", "");
			} catch (SQLException e) {
				logger.error("Fail to execute a querry to initialise "+database);
				logger.error(e.getMessage());
				ok = false;
			} catch (Exception e) {
				logger.error("Fail to connect to the new database "+database);
				logger.error(e.getMessage());
				ok = false;
			}

		}

		return ok;
	}

	/**
	 * Link a database to a user
	 * @param name
	 * @param url
	 * @return true if the database has been linked
	 */ 
	public boolean linkDatabase(String name, String url){
		if(url.isEmpty()){
			logger.warn("Cannot link to an empty url");
			return false;
		}
		prefNode.put(name, url);
		return prefNode.get(name, "").equals(url);
	}

	/**
	 * Return a Connection with the database
	 * @param name
	 * @return
	 * @throws Exception 
	 */
	public JdbcConnection get(String name) throws Exception{
		logger.debug("name of the db: '"+name+"', url: '"+prefNode.get(name, "")+"'");
		return new JdbcConnection(new JdbcHdfsPrefsDetails(prefNode.get(name, "")), new MySqlBasicStatement());
	}
}
