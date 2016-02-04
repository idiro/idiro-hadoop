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


import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.idiro.hadoop.HadoopVar;


/**
 * HBase Interface of the program
 * 
 * Warning: does not update the HBaseManager
 * If you want to update the HBaseManager please 
 * see HBaseStatement 
 * @author etienne
 *
 */
public class HBaseInterface {

	private static boolean init;
	private static HBaseInterface instance = new HBaseInterface();
	private Logger logger = Logger.getLogger(getClass());
	private HBaseAdmin hbase;
	public static final String defaultCF = "f";
	public static final String[] defaultCFList = new String[]{defaultCF};


	private HBaseInterface() {
		if(!HBaseInterface.init){
			HBaseInterface.init = true;
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", HadoopVar.get("zkQuorum"));
			conf.set("hbase.zookeeper.property.clientPort", HadoopVar.get("zkPort"));

			try {
				hbase = new HBaseAdmin(conf);
			}catch (MasterNotRunningException e) {
				logger.error("HMaster not reachable");
				logger.error(e.getMessage());
				HBaseInterface.init = false;
			} catch (ZooKeeperConnectionException e) {
				logger.error("ZooKeeper exception");
				logger.error(e.getMessage());
				HBaseInterface.init = false;
			}
		}
	}
	
	public boolean createHBaseTable(String tableName){
		return createHBaseTable(tableName, new LinkedHashSet<String>());
	}
	
	public boolean createHBaseTable(String tableName, Set<String>colFam){
		boolean ok = true;
		tableName = tableName.toLowerCase();

		//cf is the default column delimiter of the program we always add it
		for(int i = 0; i < defaultCFList.length;++i){
			colFam.add(defaultCFList[i]);
		}
		try {
			HTableDescriptor desc = new HTableDescriptor(tableName);
			Iterator<String> itColFam = colFam.iterator();
			while(itColFam.hasNext()){
				desc.addFamily(new HColumnDescriptor(itColFam.next().getBytes()));
			}
			hbase.createTable(desc);
		} catch (MasterNotRunningException e) {
			logger.error("HMaster not reachable");
			logger.error(e.getMessage());
			ok = false;
		} catch (ZooKeeperConnectionException e) {
			logger.error("ZooKeeper exception");
			logger.error(e.getMessage());
			ok = false;
		} catch (IOException e) {
			logger.error("IO exception");
			logger.error(e.getMessage());
			ok = false;
		}
		return ok;
	}

	public Set<String> getTables(){
		Set<String> res = new LinkedHashSet<String>();
		try {
			HTableDescriptor[] descs = hbase.listTables();
			for(int i =0; i < descs.length;++i){
				res.add(new String(descs[0].getName()));
			}
		} catch (MasterNotRunningException e) {
			logger.error("HMaster not reachable");
			logger.error(e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			logger.error("ZooKeeper exception");
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error("IO exception");
			logger.error(e.getMessage());
		}
		return res;
	}

	public Set<String> getColFamFrom(String table){
		Set<String> res = new LinkedHashSet<String>();
		try {
			Iterator<byte[]> famKey = hbase.getTableDescriptor(Bytes.toBytes(table)).getFamiliesKeys().iterator();
			while(famKey.hasNext()){
				res.add(new String(famKey.next()));
			}

		} catch (MasterNotRunningException e) {
			logger.error("HMaster not reachable");
			logger.error(e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			logger.error("ZooKeeper exception");
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error("IO exception");
			logger.error(e.getMessage());
		}
		return res;
	}

	public boolean addColFamFrom(String tableName,String colFam){
		boolean ok = true;
		tableName = tableName.toLowerCase();

		try {
			if(getColFamFrom(tableName).contains(colFam)){
				logger.debug("The column '"+colFam+"' already exists in '"+tableName+"'");
				ok = false;
			}else{
				hbase.getTableDescriptor(Bytes.toBytes(tableName)).addFamily(new HColumnDescriptor(colFam));
			}
		} catch (MasterNotRunningException e) {
			logger.error("HMaster not reachable");
			logger.error(e.getMessage());
			ok = false;
		} catch (ZooKeeperConnectionException e) {
			logger.error("ZooKeeper exception");
			logger.error(e.getMessage());
			ok = false;
		} catch (IOException e) {
			logger.error("IO exception");
			logger.error(e.getMessage());
			ok = false;
		}
		return ok;
	}

	public boolean deleteColFamFrom(HBaseManager hm, String tableName,String colFam){
		boolean ok = true;
		tableName = tableName.toLowerCase();

		try {
			if(!getColFamFrom(tableName).contains(colFam)){
				logger.warn("The column '"+colFam+"' does not exist in '"+tableName+"'");
				ok = false;
			}else{
				hm.deleteFeatureToTable(tableName, colFam, null);
				hbase.getTableDescriptor(Bytes.toBytes(tableName)).removeFamily(Bytes.toBytes(colFam));
			}
		} catch (MasterNotRunningException e) {
			logger.error("HMaster not reachable");
			logger.error(e.getMessage());
			ok = false;
		} catch (ZooKeeperConnectionException e) {
			logger.error("ZooKeeper exception");
			logger.error(e.getMessage());
			ok = false;
		} catch (IOException e) {
			logger.error("IO exception");
			logger.error(e.getMessage());
			ok = false;
		}
		return ok;
	}



	/**
	 * @return the instance
	 */
	public static HBaseInterface getInstance() {
		return instance;
	}


	/**
	 * @return the hbase
	 */
	public HBaseAdmin getHbase() {
		return hbase;
	}

}
