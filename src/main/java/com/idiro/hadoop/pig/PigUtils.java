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

package com.idiro.hadoop.pig;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;


public class PigUtils {
	
	public static String getDelimiter(char character){
		String delimiter = null;
		if( character >= 32 && character <= 126 ){
			delimiter = new String(new char[]{character});
		}else if(character == '\t'){
			delimiter = "\\t";
		}else{
			delimiter = Integer.toOctalString(character);
			if(delimiter.length() == 1){
				delimiter = "\\\\u00"+delimiter;
			}else if(delimiter.length() == 2){
				delimiter = "\\\\u0"+delimiter;
			}else{
				delimiter = "\\\\u"+delimiter;
			}
		}
		return delimiter;
	}

	public static String getLoadLineQuery(Path dataPath, String dataFormat, String delimiter, Map<String,String> features) {
		String query = "IN = LOAD '" + dataPath.toString() + "' USING ";
		if (dataFormat.equals("TEXTFILE")  || dataFormat.equals("COMPRESSED")) {
			query += "PigStorage('"
					+ PigUtils.getDelimiter(delimiter.charAt(0))
					+ "') ";
		} else if (dataFormat.equals("BINFILE")) {
			query += "' USING BinStorage() ";
		}
		query += " AS (";
		Iterator<String> it = features.keySet().iterator();
		while(it.hasNext()){
			String featureName = it.next();
			query += featureName
					+ ":"
					+ convertToPigType(features.get(featureName)) + ",";
		}
		query = query.substring(0, query.length() - 1);
		query += ");\n\n";
		return query;
	}
	

	public static String convertToPigType(String type) {
		if (type.equalsIgnoreCase("STRING")) {
			type = "CHARARRAY";
		}else if(type.equalsIgnoreCase("DATE")){
			type = "DATETIME";
		}else if(type.equalsIgnoreCase("TIMESTAMP")){
			type = "DATETIME";
		}else if(type.equalsIgnoreCase("CHAR")){
			type = "CHARARRAY";
		}else if(type.equalsIgnoreCase("CATEGORY")){
			type = "CHARARRAY";
		}
		return type.toLowerCase();
	}

}
