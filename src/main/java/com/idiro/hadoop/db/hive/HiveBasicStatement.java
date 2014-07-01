package com.idiro.hadoop.db.hive;


import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.idiro.utils.db.BasicStatement;


/**
 * Hive Basic Statement implementation.
 * 
 * Implements the basic statements for hive queries.
 * 
 * 
 * @author etienne
 *
 */
public class HiveBasicStatement implements BasicStatement{

	private Logger logger = Logger.getLogger(getClass());


	/**
	 * Initialise the driver needed for the class
	 * 
	 * @throws ClassNotFoundException
	 */
	public HiveBasicStatement() throws ClassNotFoundException{
		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
	}


	/**
	 * Creates an external table.
	 * Creates an external table, no partitions allowed
	 * In this versions there are 2 options
	 * the delimiter in ASCII
	 * the path LOCATION both of them are mandatory
	 */
	@Override
	public String createExternalTable(String tableName,
			Map<String, String> features, String[] options) {

		if(options.length != 2){
			logger.warn("The hive createExternalTable has two mandatory options: a delimiter in ASCII and a location");
		}
		String delimiter = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '"+options[0]+"'";
		String location = "LOCATION '"+options[1]+"'";

		String stmt_str = "CREATE EXTERNAL TABLE "+tableName+"(\n\t";

		Iterator<String> it = features.keySet().iterator();
		if(!it.hasNext()){
			logger.warn("Table does not contain any features");
		}else{
			String feature = it.next();
			stmt_str += feature+" "+features.get(feature);
			while(it.hasNext()){
				feature = it.next();
				stmt_str += ",\n\t"+feature+" "+features.get(feature);
			}
		}

		stmt_str +="\n)\n"+delimiter+"\n"+"STORED AS TEXTFILE"+"\n"+location;

		return stmt_str;
	}

	/**
	 * Creates a table.
	 * In this versions the options are a maximum of 3 in this order
	 * partition: comma delimited partition name
	 * storage method the keyword associated with a storage method
	 * clustering method 3 field comma delimited : cluster, sort, number of bucket
	 */
	@Override
	public String createTable(String tableName,
			Map<String, String> features, String[] options) {
		String partition="";
		String cluster ="";
		String storage ="";
		if(options != null && options.length > 0){
			if(!options[0].isEmpty()){

				String[] partitions = options[0].split(",");
				partition = "PARTITIONED BY ("+partitions[0]+" STRING";
				for(int i =1; i < partitions.length; ++i){
					partition += ", "+partitions[i]+" STRING";
				}
				partition += ")";
			}
			if(options.length > 1){
				if(!options[1].isEmpty())
					storage = "STORED AS "+options[1];

				if(options.length > 2){
					if(!options[2].isEmpty()){
						String[] fields = options[2].split(",");
						if(fields.length != 3){
							logger.warn("The createTable 3 options needed 3 comma delimited field");
						}
						cluster =  "CLUSTERED BY("+fields[0]+") SORTED BY("+fields[1]+") INTO "+fields[2]+" BUCKETS";
					}
					if(options.length > 3){
						logger.warn("The hive createtable method does not take more than 3 options");
					}
				}
			}
		}


		String stmt_str = "CREATE TABLE "+tableName+" (\n\t";

		Iterator<String> it = features.keySet().iterator();
		if(!it.hasNext()){
			logger.warn("Table does not contain any features");
		}else{
			String feature = it.next();
			stmt_str += feature+" "+features.get(feature);
			while(it.hasNext()){
				feature = it.next();
				stmt_str += ",\n\t"+feature+" "+features.get(feature);
			}
		}

		stmt_str +="\n)"+partition+" "+cluster+" "+storage;


		return stmt_str;
	}

	@Override
	public String deleteTable(String tableName) {
		return "DROP TABLE "+tableName;
	}

	/**
	 * Export a table to a map reduce directory.
	 * 
	 * One option mandatory: the path where to create the table
	 */
	@Override
	public String exportTableToFile(String tableNameFrom,
			Map<String, String> features, String[] options) {

		String path = "";

		if(options.length != 1){
			logger.warn("Hive Export takes one and only one parameter: the path where to export");
		}
		path = options[0].trim();
		if( !path.endsWith("/")){
			path +="/";
		}

		String stmt_str = "INSERT OVERWRITE DIRECTORY '"+path+"'\n"; 

		stmt_str += "SELECT ";
		Iterator<String> it = features.keySet().iterator();
		if(!it.hasNext()){
			logger.warn("Export 0 features");
		}else{
			stmt_str += it.next();
			while(it.hasNext()){
				stmt_str += ",\n\t"+it.next();
			}
		}
		stmt_str +="\nFROM "+tableNameFrom;

		return stmt_str;
	}

	/**
	 * Query to show all the tables.
	 * 
	 * @return
	 */
	@Override
	public String showAllTables() {
		return "SHOW TABLES";
	}

	/**
	 * Query to describe a table.
	 * 
	 * @return
	 */
	@Override
	public String showFeaturesFrom(String tableName) {
		return "DESCRIBE "+tableName;
	}

	

}
