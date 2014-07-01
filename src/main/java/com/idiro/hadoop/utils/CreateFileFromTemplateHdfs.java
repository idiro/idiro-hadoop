package com.idiro.hadoop.utils;


import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.idiro.hadoop.NameNodeVar;
import com.idiro.utils.CreateFileFromTemplate;

/**
 * Create a new file from a template in hdfs.
 * @see com.idiro.utils.CreateFileFromTemplate
 * 
 * This file contains a main method
 *  
 * @author etienne
 *
 */
public class CreateFileFromTemplateHdfs {

	public boolean create(String templateFile, String outputFile,Map<String,String> words) throws Exception{
		boolean ok = true;
		Configuration conf = NameNodeVar.getConf();
		FileSystem dfs = org.apache.hadoop.fs.FileSystem.get(conf);

		File locFrom = File.createTempFile("idiro", "from");
		File locTo = File.createTempFile("idiro", "to");

		Path fileTo, fileFrom, fileLocFrom, fileLocTo;
		fileFrom = new Path(templateFile);
		fileLocFrom = new Path(locFrom.getAbsolutePath());
		fileTo = new Path(outputFile);
		fileLocTo = new Path(locTo.getAbsolutePath());

		if(new File(locFrom.getAbsolutePath()).exists()){
			new File(locFrom.getAbsolutePath()).delete();
		}

		if(!dfs.exists(fileFrom)){
			ok = false;
		}else{
			dfs.copyToLocalFile(fileFrom, fileLocFrom);
			ok = new CreateFileFromTemplate().create(locFrom.getAbsolutePath(),locTo.getAbsolutePath(),words);
			dfs.copyFromLocalFile(fileLocTo,fileTo);

			new File(locFrom.getAbsolutePath()).delete();
			new File(locTo.getAbsolutePath()).delete();
		}
		return ok;
	}

	/**
	 * Create a file from a template.
	 * @see com.idiro.utils.CreateFileFromTemplate#main(String[])
	 * Needs to add as first argument the hadoop namenode
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		if(args.length < 3){
			throw new Exception("Needs at least 3 args: the namenode, the template and the output file");
		}
		//Initialise conf
		NameNodeVar.set(args[0]);
		Configuration conf = NameNodeVar.getConf();
		FileSystem dfs = org.apache.hadoop.fs.FileSystem.get(conf);

		Path in = new Path(args[1]);
		Path out = new Path(args[2]);
		if(!dfs.exists(in)){
			throw new Exception(args[0]+" does not exists");
		}
		if(dfs.exists(out)){
			dfs.delete(out,true);
		}

		Map<String,String> words = new LinkedHashMap<String,String>();
		for(int i = 3; i < args.length;++i){
			String[] split = args[i].split("\\|");
			if(split.length != 2){
				throw new Exception("Dictionary not conformed, "+split.length+" number of field instead of 2");
			}
			words.put(split[0],split[1]);
		}

		if(! new CreateFileFromTemplateHdfs().create(args[1],args[2],words)){
			throw new Exception("Did not succeed to create file from template");
		}

	}
}
