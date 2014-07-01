package com.idiro.hadoop.checker;


import java.util.Collection;
import java.util.Set;

import com.idiro.check.Checker;
import com.idiro.hadoop.db.hbase.HBaseStatement;

/**
 * HBaseChecker check hbase tables contained in a metastore.
 * 
 * The metastore is given indirectly via the HBaseStatement object.
 * All the checks are done in the metastore. The metastore is kept
 * up to date automatically within the code. However it cannot take
 * into account external/manual changes.
 * 
 * @author etienne
 *
 */
public class HBaseChecker extends Checker{

	/**
	 * HBaseStatement to execute the queries
	 */
	private HBaseStatement bs;
	
	
	public HBaseChecker(HBaseStatement bs){
		this.bs = bs;
		initialized = true;
	}
	
	/**
	 * Check if all the tables in the list exist in the database
	 * return false if not
	 * @param tablenames tables to check
	 * @return true if all the tables exist
	 */
	public boolean areTablesExist(Collection<String> tablenames){
		Set<String> tables = bs.getAllTables();
		logger.trace("Check tables exist");
		boolean ok = tables.containsAll(tablenames);
		if(!ok){
			logger.debug(tables+" does not contains all "+tablenames);
		}
		return ok;
	}
	
	/**
	 * Check if the hbase table exist
	 * @param tablename
	 * @return
	 */
	public boolean isTableExist(String tablename){
		Set<String> tables = bs.getAllTables();
		logger.trace("Check table exist");
		boolean ok = tables.contains(tablename);
		if(!ok){
			logger.debug(tables+" does not contains '"+tablename+"'");
		}
		return ok;
	}
	
	/**
	 * Check if the features are contained
	 * @param tableName
	 * @param features
	 * @return
	 */
	public boolean areFeaturesContained(String tableName,Collection<String> features){
		if(!isTableExist(tableName)){
			logger.debug("'"+tableName+"' does not exists");
			return false;
		}
		Set<String> tableFeats = bs.getFeatures(tableName); 
		boolean ok = tableFeats.containsAll(features);
		if(!ok){
			logger.debug("In '"+tableName+"' "+tableFeats+" does not contain all "+features);
		}
		return ok;
	}
	
	/**
	 * Returns true if the features are exactly the same in the too list
	 * @param tableName
	 * @param features
	 * @return
	 */
	public boolean areFeaturesTheSame(String tableName,Collection<String> features){
		if(!isTableExist(tableName)){
			logger.debug("'"+tableName+"' does not exists");
			return false;
		}
		Set<String> tableFeats = bs.getFeatures(tableName); 
		boolean ok = tableFeats.containsAll(features) &&
				features.containsAll(bs.getFeatures(tableName));
		if(!ok){
			logger.debug("In '"+tableName+"' "+tableFeats+" does not contains the same features than"+features);
		}
		
		return ok;
	}
	
	/**
	 * Check if a features is contained
	 * @param tableName
	 * @param feature
	 * @return
	 */
	public boolean isFeatureContained(String tableName,String feature){
		if(!isTableExist(tableName)){
			logger.debug("'"+tableName+"' does not exists");
			return false;
		}
		Set<String> tableFeats = bs.getFeatures(tableName); 
		boolean ok = tableFeats.contains(feature);
		if(!ok){
			logger.debug("In '"+tableName+"' "+tableFeats+" does not contains '"+feature+"'");
		}
		return ok;
	}
}
