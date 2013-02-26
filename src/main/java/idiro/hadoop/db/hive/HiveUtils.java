package idiro.hadoop.db.hive;

import idiro.check.DbChecker;
import idiro.hadoop.NameNodeVar;
import idiro.utils.db.JdbcConnection;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Hive Utils class.
 * 
 * Implements tools that may be useful
 * to interact with a Hive Database.
 * This class complete HiveBasicStatement
 * ( @see idiro.hadoop.db.hive.HiveBasicStatement )
 * 
 * @author etienne
 *
 */
public class HiveUtils {

	
	static private Logger logger = Logger.getLogger(HiveUtils.class);
	
	/**
	 * Export a hive table to a data file.
	 * 
	 * 
	 * @param conn Hive Connection
	 * @param tableName the table name
	 * @param out the data file
	 * @param features the features with their type
	 * @return
	 */
	public static boolean exportTable(JdbcConnection conn, String tableName, Path out,
			Map<String, String> features){
		boolean ok = true;

		String[] options = {out.toString()};		

		try {
			conn.exportTableToFile(tableName, features, options);
		} catch (SQLException e) {
			logger.debug(e.getMessage());
			return false;
		}


		return ok;

	}

	/**
	 * Import a hive table from a data file.
	 * 
	 * The data file is suppose to be checked before.
	 * 
	 * @param conn Hive Connection
	 * @param tableName the table name
	 * @param features the features with their type
	 * @param in the input data file
	 * @param tablePath the external table path
	 * @param delimiter the delimiter
	 * @return
	 */
	public static boolean importTable(JdbcConnection conn, 
			String tableName, Map<String,String> features,
			Path in, 
			Path tablePath, 
			char delimiter) {

		boolean ok = true;
		
		try {
			HiveBasicStatement bs = new HiveBasicStatement();
			DbChecker dbCh = new DbChecker(conn);
			if(! dbCh.isTableExist(tableName)){
				logger.debug("The table which has to be imported has not been created");
				logger.debug("Creation of the table");
				Integer ASCIIVal = (int) delimiter;
				String[] options = { ASCIIVal.toString() ,
						tablePath.toString()};
				conn.executeQuery(bs.createExternalTable(tableName, features, options));
			}else{
				//Check if it is the same table 
				if( ! dbCh.areFeaturesTheSame(tableName, features.keySet())){
					logger.warn("Mismatch between the table to import and the table in the database");
					return false;
				}

				ResultSet res = conn.executeQuery("DESCRIBE EXTENDED "+tableName);
				boolean found = false;
				String stat_str,
				search_str = "field.delim=";

				logger.debug("Search delimiter for table "+tableName);
				while(res.next() && !found){
					int i = 1;
					try{
						while( (stat_str = res.getString(i)) != null && !found){
							if(stat_str.contains(search_str)){
								found = true;
								stat_str = stat_str.substring(
										stat_str.indexOf(search_str)+
										search_str.length()
										);
								stat_str = stat_str.substring(
										0, stat_str.indexOf('}')
										);
								if(stat_str.length() != 1 || stat_str.charAt(0) != delimiter){

									logger.warn("The delimiter in the table '"+stat_str+
											"' and the delimiter declared '"+
											delimiter+"' does not match");
									logger.debug("Length of the delimiter '"+
											stat_str.length()+"' ASCII value '"+
											(int)stat_str.charAt(0)+"'");
									logger.debug(
											"ASCII value of the delimiter declared '"+
													(int) delimiter+"'");
									ok = false;
								}
							}
							++i;
						}
					} catch (SQLException e) {
					}
				}

				if(!found || !ok){
					if(!found){
						logger.warn("Did not find the delimiter definition in "+tableName);
					}
					return false;
				}


			}
		} catch (SQLException e) {
			logger.debug("Fail to watch the datastore");
			logger.debug(e.getMessage());
			return false;
		} catch (ClassNotFoundException e1) {
			logger.error("Cannot instanciate Hive statement");
			logger.error(e1.getMessage());
			return false;
		}

		if(ok){
			logger.info("copy to map red dir");
			try {
				FileSystem fs = NameNodeVar.getFS();
				FileUtil.copy(fs, in,
						fs, tablePath, 
						false, NameNodeVar.getConf());

			} catch (IOException e1) {
				logger.debug(e1.getMessage());
				return false;
			}
		}
		return ok;
	}
}
