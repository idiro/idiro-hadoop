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

package com.idiro.hadoop.checker;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.idiro.check.Checker;
import com.idiro.hadoop.NameNodeVar;

/**
 * Checks file in hdfs.
 * 
 * Allows to check the basics about
 * a path and keep logs.
 * 
 * @author etienne
 *
 */
public class HdfsFileChecker extends Checker{

	/**
	 * Filesystem
	 */
	private FileSystem fS;
	
	/**
	 * Path
	 */
	private Path path;
	/**
	 * File Canonical name of the file
	 */
	private String fileCanonicalName;

	/**
	 * @param file file to check
	 */
	public HdfsFileChecker(Path path) {
		initialized = init(path);
	}

	/**
	 * @param filename name of the file to check
	 */
	public HdfsFileChecker(String filename) {
		if(filename != null){
			initialized = init( new Path(filename));
		}else{
			logger.error("Try to check a file which has no name");
			initialized = false;
		}
	}

	/**
	 * Initialise a HdfsFileChecker instance 
	 * @param file
	 * @return
	 */
	protected boolean init(Path path){
		boolean init = true;
		try {
			fS = NameNodeVar.getFS();
			this.path = path;
			fileCanonicalName = path.getName();
		} catch (IOException e) {
			logger.error("Cannot initialise the hdfs file system");
			logger.error(e.getMessage());
			init = false;
		}
		return init;
	}


	/**
	 * Check if the file exists or not
	 * @return true if the file exists
	 */
	public boolean exists() {
		boolean exist = false;
		try {
			exist = fS.exists(path);
			logger.debug("check existence of "+fileCanonicalName+", result: "+exist);	
		} catch (Exception e) {
			systemErrorLog();
		}
		return exist;
	}

	/**
	 * Check if the file is a directory
	 * @return true if the file is a directory
	 */
	public boolean isDirectory(){
		boolean isDir = false;
		try {
			if(exists()){
				isDir = fS.getFileStatus(path).isDir();
			}
			logger.debug("check if "+fileCanonicalName+" is a directory, result: "+isDir);

		} catch (Exception e) {
			systemErrorLog();
		}
		return isDir;
	}

	/**
	 * Check if the file is a file (opposed to a directory)
	 * @return true if it is
	 */
	public boolean isFile(){
		boolean isF = false;
		try {
			if(exists()){
				isF = !fS.getFileStatus(path).isDir();
			}
			logger.debug("check if "+fileCanonicalName+" is a file, result: "+isF);
		} catch (Exception e) {
			systemErrorLog();
		}
		return isF;
	}

	/**
	 * Check the size of a file
	 * @return the size
	 */
	public long getSize(){
		long size = 0L;
		try {
			size = fS.getFileStatus(path).getLen();
			logger.debug("check the size of "+fileCanonicalName+", result: "+size);
		} catch (Exception e) {
			systemErrorLog();
		}
		return size;
	}

	
	/**
	 * Send a log for a system exception
	 */
	private void systemErrorLog(){
		logger.error("File "+fileCanonicalName+": system exception, please check the right of the path and the system settings");
	}

	/**
	 * @return the path
	 */
	public Path getPath() {
		return path;
	}

	/**
	 * @return the filename
	 */
	public String getFilename() {
		return fileCanonicalName;
	}

	/**
	 * @param file the file to set
	 */
	public void setPath(Path path) {
		initialized = init(path);
	}
	
	public void close(){
		try {
			fS.close();
		} catch (IOException e) {
			logger.warn("FileSystem did not close corectly");
		}
	}
}
