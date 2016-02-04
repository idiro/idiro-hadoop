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

package com.idiro.hadoop.db.hbase;


import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import com.idiro.hadoop.NameNodeVar;
import com.idiro.hadoop.checker.HBaseChecker;
import com.idiro.hadoop.hbasem.HBaseInterface;
import com.idiro.hadoop.hbasem.HBaseManager;

/**
 * Class interface with HBase.
 * 
 * This Class link the HBaseManager (metastore),
 * @see com.idiro.hadoop.hbasem.HBaseManager
 * and the HBaseInterface (interface for hbase),
 * @see com.idiro.hadoop.hbasem.HBaseInterface.
 * It is the higher level of interface within the library
 * where the hbase fields management (column family and identifier) 
 * is hidden to the developer but it can retrieve them through getters.
 * 
 * @author etienne
 *
 */
public class HBaseStatement{

	/**
	 * Name of the Database manager
	 */
	private String dbName;

	/**
	 * HBase manager
	 */
	private HBaseManager hm;

	Logger logger = Logger.getLogger(getClass());


	/**
	 * Constructor, needs the name of the metastore.
	 * 
	 * This name will then be modified as '"hbaseM_"+name'
	 * 
	 * @param workingGroupName
	 * @throws Exception Cannot connect to the metastore database
	 */
	public HBaseStatement(String workingGroupName) throws Exception{
		this.dbName = "hbaseM_"+workingGroupName;
		hm = new HBaseManager(workingGroupName);
	}

	/**
	 * Get the name of all the tables
	 * 
	 * @return
	 */
	public Set<String> getAllTables() {
		ResultSet q;
		Set<String> res = new LinkedHashSet<String>();
		try {
			q = hm.getConn().executeQuery(
					"SHOW TABLES where " +
							"tables_in_"+dbName+" not like 'hbase_category' " +
							"AND tables_in_"+dbName+" not like 'hbase_dictionary'");

			while(q.next()){
				res.add(q.getString("tables_in_"+dbName));
			}
		} catch (SQLException e) {
			logger.error("Error to execute a query");
			logger.error(e.getMessage());
		}
		return res;
	}
	
	public Set<String> getTables(String regex){
		Iterator<String> allTables = getAllTables().iterator();
		Set<String> ans = new LinkedHashSet<String>();
		while(allTables.hasNext()){
			String cur = allTables.next();
			if(cur.matches(regex)){
				ans.add(cur);
			}
		}
		return ans;
	}

	/**
	 * Get the name of the hbase fields of these features.
	 * 
	 * The hbase field is composed of the column family 
	 * following by the field name separated by ':'
	 *  
	 * @param tableName
	 * @param features
	 * @return
	 */
	public Set<String> getHbaseFields(String tableName, Set<String> features){
		HBaseChecker hbCh = new HBaseChecker(this);
		Set<String> res = new LinkedHashSet<String>();
		if(!hbCh.isTableExist(tableName)){
			logger.warn("Table "+tableName+" does not exists");
			return res;
		}

		if(!hbCh.areFeaturesContained(tableName, features)){
			logger.warn("All features are not here");
			return res;
		}

		Map<String,String> hbaseFeat = hm.getTable(tableName);
		Iterator<String> it = features.iterator();
		while(it.hasNext()){
			String id = hm.getFeatureDesc(it.next()).get(1);
			res.add(hbaseFeat.get(id)+":"+id);
		}
		return res;
	}

	/**
	 * Get the name of a hbase field.
	 * 
	 * The hbase field is composed of the column family 
	 * following by the field name separated by ':'
	 * @param tableName
	 * @param feature
	 * @return
	 */
	public String getHbaseField(String tableName, String feature){
		Set<String> in = new LinkedHashSet<String>();
		in.add(feature);
		return getHbaseFields(tableName,in).iterator().next();
	}

	/**
	 * Get the name of a hbase field, sorted by feature name.
	 * 
	 * The hbase field is composed of the column family 
	 * following by the field name separated by ':'
	 * @param tableName
	 * @param features
	 * @return <key: feature name, value: hbaes field>
	 */
	public Map<String,String> getFeatWithHbaseField(String tableName, Collection<String> features){
		HBaseChecker hbCh = new HBaseChecker(this);
		Map<String,String> res = new LinkedHashMap<String,String>();
		if(!hbCh.isTableExist(tableName)){
			logger.warn("Table "+tableName+" does not exists");
			return res;
		}

		if(!hbCh.areFeaturesContained(tableName, features)){
			logger.warn("All features are not heres");
			return res;
		}

		Map<String,String> hbaseFeat = hm.getTable(tableName);
		Iterator<String> it = features.iterator();
		while(it.hasNext()){
			String feat = it.next();
			String id = hm.getFeatureDesc(feat).get(1);
			res.put(feat,hbaseFeat.get(id)+":"+id);
		}
		return res;
	}


	/**
	 * Get all the features contained in a table.
	 * @param tableName
	 * @return
	 */
	public Set<String> getFeatures(String tableName) {
		return new LinkedHashSet<String>(hm.getFeaturesFromId(hm.getTable(tableName).keySet()).values());
	}

	/**
	 * Get all the features with their type of a table.
	 * @param tableName
	 * @return
	 */
	public Map<String,String> getFeaturesWithType(String tableName){
		Map<String,String> res = new LinkedHashMap<String,String>();
		Iterator<String> it = getFeatures(tableName).iterator();
		while(it.hasNext()){
			String feat = it.next();
			res.put(feat, hm.getFeatureDesc(feat).get(2));
		}
		return res;
	}

	/**
	 * Deletes a table.
	 * 
	 * The delete statement is made in two steps.
	 * We delete the hbase table effectively in this methods.
	 * Then we return the string to drop the table in the metastore
	 */
	public boolean deleteTable(String tableName) {
		boolean ok = false;
		HBaseChecker hbCh = new HBaseChecker(this);
		if(hbCh.isTableExist(tableName)){
			try {
				HBaseInterface.getInstance().getHbase().disableTable(tableName);
				HBaseInterface.getInstance().getHbase().deleteTable(tableName);
				hm.getConn().deleteTable(tableName);
				ok = true;
			} catch (IOException e) {
				logger.error("Cannot delete the table"+tableName);
				logger.error(e.getMessage());
			} catch (SQLException e) {
				logger.error("Cannot execute a query");
				logger.error(e.getMessage());
			}
		}

		return ok;
	}

	/**
	 * Deletes a list of feature from a table.
	 * 
	 * It does not delete really the data but only
	 * from the metastore. To destroy the data you
	 * need to remove the table.
	 * 
	 * @param tableName
	 * @param features
	 * @return
	 */
	public boolean deleteFeatures(String tableName,Collection<String> features){
		boolean ok = true;
		HBaseChecker hbCh = new HBaseChecker(this);
		if(hbCh.isTableExist(tableName)){
			Map<String,String> featureWithColFam = getFeatWithHbaseField(tableName,features);
			Iterator<String> itF = featureWithColFam.keySet().iterator();
			while(itF.hasNext() && ok){
				String id = featureWithColFam.get(itF.next());
				logger.info("Remove hbase field id: "+id.split(":")[1]);
				ok = hm.deleteFeatureToTable(tableName, "", id.split(":")[1]);
			}
		}else{
			ok = false;
		}
		return ok;
	}
	
	
	public boolean deleteAll(Map<String,String> features){
		boolean ok = true;
		try {
			HBaseChecker hbCh = new HBaseChecker(this); 
			List<String> tables = hm.getTables();
			Collection<String> featuresToDelete = hm.getMapFeatures(features).values();
			Iterator<String> it = tables.iterator();
			while(it.hasNext() && ok){
				String tablename = it.next();
				Map<String,String> tableFeatures = hm.getFeaturesFromId(hm.getTable(tablename).keySet());
				List<String> tableFeaturesToDelete = new LinkedList<String>();
				Iterator<String> hbaseIdIt = tableFeatures.keySet().iterator();
				while(hbaseIdIt.hasNext()){
					String hbid = hbaseIdIt.next();
					if(featuresToDelete.contains(hbid)){
						tableFeaturesToDelete.add(tableFeatures.get(hbid));
					}
				}
				
				if(!tableFeaturesToDelete.isEmpty()){
					if(hbCh.areFeaturesTheSame(tablename, tableFeaturesToDelete)){
						ok = deleteTable(tablename);
					}else{
						ok = deleteFeatures(tablename, tableFeaturesToDelete);
					}
				}
				
			}
			
			if(ok){
				hm.removeFeaturesFromDicTable(features);
			}
		} catch (Exception e) {
			logger.error("Cannot connect to the HBase manager");
			logger.error(e.getMessage());
			ok = false;
		}
		return ok;
	}

	/** 
	 * Create a table.
	 * 
	 * The final step is for effectively register the change in
	 * the metastore.
	 * 
	 */
	public boolean createTable(String tableName, Map<String, String> features) {
		boolean ok = true;

		HBaseChecker hbCh = new HBaseChecker(this);
		if(hbCh.isTableExist(tableName)){
			ok = false;
		}else{
			logger.info("Create table "+tableName);
			
			//Get the features (create if necessary)
			Map<String,String> featNameWithId = hm.getMapFeatures(features);
			logger.info("The features are: \nKey"+featNameWithId.keySet()+", \nvalue:"+featNameWithId.values());

			//Linked each hbase id to the default column family
			Map<String,String> idWithColFam = new LinkedHashMap<String,String>();
			Iterator<String> it = featNameWithId.keySet().iterator();
			while(it.hasNext()){
				idWithColFam.put(featNameWithId.get(it.next()), HBaseInterface.defaultCF);
			}

			logger.debug("create table with hbaseIds: "+idWithColFam.keySet());

			//Create the hbase table itself
			ok = HBaseInterface.getInstance().createHBaseTable(tableName);

			//Create the hbase table manager
			if(ok)
				ok = hm.addFeatureToTable(tableName, idWithColFam);

		}
		return ok;
	}

	/**
	 * Add features to a table.
	 * 
	 * Add the features that does not exists into a hbase table.
	 * It ignores the features that exists.
	 * 
	 * @param tableName
	 * @param features
	 * @return
	 */
	public boolean addFeatures(String tableName, Map<String, String> features) {
		boolean ok = true;

		HBaseChecker hbCh = new HBaseChecker(this);
		if(!hbCh.isTableExist(tableName)){
			ok = false;
		}else{
			Map<String,String> featuresNotExist = new LinkedHashMap<String,String>();
			Iterator<String> it = features.keySet().iterator();
			while(it.hasNext()){
				String cur = it.next();
				if(!hbCh.isFeatureContained(tableName, cur)){
					featuresNotExist.put(cur,features.get(cur));
				}
			}
			logger.debug("update table with features: "+featuresNotExist.keySet());
			//Get the features (create if necessary)
			Map<String,String> featNameWithId = hm.getMapFeatures(featuresNotExist);

			//Linked each hbase id to the default column family
			Map<String,String> idWithColFam = new LinkedHashMap<String,String>();
			it = featNameWithId.keySet().iterator();
			while(it.hasNext()){
				idWithColFam.put(featNameWithId.get(it.next()), HBaseInterface.defaultCF);
			}

			logger.debug("update table with hbaseIds: "+idWithColFam.keySet());

			//Create the hbase table manager
			ok = hm.addFeatureToTable(tableName, idWithColFam);
		}
		return ok;
	}	


	/**
	 * Load a hbase table into a map reduce directory.
	 * 
	 * Load a hbase table and its specified features
	 * into a map reduce directory. This method uses
	 * pig.
	 * 
	 * 
	 * @param tableNameFrom the table to load
	 * @param features the features to load
	 * @param mapRedout the map reduce output directory
	 * @param delimiterOut the field delimiter
	 * @return true if process ok
	 */
	public boolean exportTable(String tableNameFrom,
			Map<String, String> features, Path mapRedout, char delimiterOut){

		FileSystem fs;
		try {
			fs = NameNodeVar.getFS();
			fs.delete(mapRedout,true);
			fs.close();
		} catch (IOException e1) {
			logger.error("Error with the hdfs api");
			logger.error(e1.getMessage());
			return false;
		}


		Map<String, String> featureWithId = hm.getMapFeatures(features);
		Map<String, String> idWithColFam = hm.getTable(tableNameFrom);
		Map<String, String> filteredIdWithFam = new LinkedHashMap<String,String>();
		Iterator<String> it = featureWithId.keySet().iterator();
		while(it.hasNext()){
			String id = featureWithId.get(it.next());
			filteredIdWithFam.put(id, idWithColFam.get(id));
		}

		String query = "x = LOAD 'hbase://"+tableNameFrom+"' USING "+
				"org.apache.pig.backend.hadoop.hbase.HBaseStorage('";
		String qFeat = ""; 
		it = filteredIdWithFam.keySet().iterator();
		while(it.hasNext()){
			String id = it.next();
			qFeat += filteredIdWithFam.get(id)+":"+id+" ";
		}
		query +=qFeat + "','-loadKey true');";


		PigServer pig;
		try {
			pig = new PigServer(ExecType.MAPREDUCE);
			logger.debug("Pig query to launch: \n"+query);
			pig.registerQuery(query);
			logger.debug("Launch the pig store statement...");
			pig.store("x", mapRedout.toString(),"PigStorage('"+
					getPigCharRepresentation(delimiterOut)+
					"')");
		} catch (ExecException e) {
			logger.error("Fail to execute pig query");
			logger.error(e.getMessage());
			return false;
		} catch (IOException e) {
			logger.error("Fail to execute pig query");
			logger.error(e.getMessage());
			return false;
		}
		return true;
	}


	/**
	 * Load a hbase table from a map-reduce directory.
	 * 
	 * Load a map reduce directory into a hbase table.
	 * This method uses pig.
	 * 
	 * @param tableNameTo table name to load to
	 * @param features the features name in order
	 * @param mapRedDirFrom the map reduce directory to load from
	 * @param delimiterIn the delimiter used in the map-reduce directory
	 * @return true if process ok
	 */
	public boolean importTable(String tableNameTo,
			Map<String, String> features, Path mapRedDirFrom, 
			char delimiterIn){
		boolean ok = true;
		PigServer pig;

		String key = features.keySet().iterator().next();

		if(!features.get(key).equalsIgnoreCase("string")){
			logger.error("A hbase key has to be a string");
			return false;
		}
		features.remove(key);

		HBaseChecker hbCh = new HBaseChecker(this);
		if(!hbCh.isTableExist(tableNameTo)){
			ok = createTable(tableNameTo, features);
		}else{
			ok = addFeatures(tableNameTo, features);
		}
		if(ok){

			Map<String, String> featureWithId = hm.getMapFeatures(features);
			Map<String, String> idWithColFam = hm.getTable(tableNameTo);
			Map<String, String> filteredIdWithFam = new LinkedHashMap<String,String>();
			Iterator<String> it = features.keySet().iterator();
			while(it.hasNext()){
				String id = featureWithId.get(it.next());
				filteredIdWithFam.put(id, idWithColFam.get(id));
			}
			logger.info("features: "+featureWithId.keySet());
			logger.info("ids: "+filteredIdWithFam.keySet());
			logger.info("column families: "+filteredIdWithFam.values());

			String hbaseFeat = ""; 
			it = filteredIdWithFam.keySet().iterator();
			while(it.hasNext()){
				String id = it.next();
				hbaseFeat += filteredIdWithFam.get(id)+":"+id+" ";
			}
			String query = "A = LOAD '"+mapRedDirFrom.toString()+"' USING PigStorage('"+
					getPigCharRepresentation(delimiterIn)+
					"');";

			try {
				pig = new PigServer(ExecType.MAPREDUCE);
				logger.debug("Pig query to launch: \n"+query);
				pig.registerQuery(query);
				logger.debug("Store process...");
				ok = pig.store("A", "hbase://"+tableNameTo,"org.apache.pig.backend.hadoop.hbase.HBaseStorage('"+hbaseFeat+"')"
						).hasCompleted(); 
			} catch (ExecException e) {
				logger.error("Fail to execute pig query");
				logger.error(e.getMessage());
				ok = false;
			} catch (IOException e) {
				logger.error("Fail to execute pig query");
				logger.error(e.getMessage());
				ok = false;
			}
		}
		return ok;
	}
	
	
	protected String getPigCharRepresentation(char delimiter){
		String ans = null;
		if(delimiter > 31 && delimiter < 127){
			ans = Character.toString(delimiter);
		}else{
			ans = String.format ("\\u%04x", (int)delimiter);
		}
		return ans;
	}

}
