package com.idiro.hadoop.utils;


import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import com.idiro.hadoop.NameNodeVar;

/**
 * Stores and retrieves preferences.
 * 
 * This class is suppose to give the same
 * main features as Preferences 
 * ( @see java.util.prefs.Preferences )
 * for hdfs.
 * 
 * You can save preferences for a user or
 * for the system. These preferences are 
 * saved under the form of a tree in hdfs.
 * The directories are "/etc/java/prefs" and
 * "/user/{username}/.prefs/java/".
 * 
 * 
 * Compare to java.util.prefs.Preferences, the
 * values are stored as "'key'='value'", like a
 * property file. The key cannot contains any '='
 * sign.
 * 
 * Warning: the name node has to be initialised
 * before using this class.
 * 
 * WARNING: There is no warranty in the preference
 * functionality to be thread safe in writing. If
 * you need to write a new preference it is advised
 * to stop all the processes that may use these preferences.
 * 
 * To change a user preference: no user process.
 * To change a system preference: no process at all.
 * 
 * @author etienne
 *
 */
public class HadoopPreferences {

	private Logger logger = Logger.getLogger(HadoopPreferences.class);
	private static Configuration conf;

	private Path path;

	/**
	 * Read/Write in the clazz package directory, system side.
	 * 
	 * Extract the package of the class and read/write in
	 * "/etc/java/prefs/{package}", careful in most of the 
	 * system you will need administration right to write
	 * in these directories.
	 * 
	 * @param clazz
	 * @return
	 */
	public static HadoopPreferences systemNodeForPackage(Class<?> clazz){
		return new HadoopPreferences(
				new Path("/etc/java/prefs/"+
						clazz.getPackage().getName().replace('.', '/')+"/package.properties"));
	}

	/**
	 * Read/Write in the clazz package directory, user side.
	 * 
	 * Extract the package of the class and read/write in
	 * "/user/{user}/.prefs/java/{package}"
	 * 
	 * @param clazz
	 * @return
	 */
	public static HadoopPreferences userNodeForPackage(Class<?> clazz){
		return new HadoopPreferences(
				new Path("/user/"+System.getProperty("user.name")+"/.prefs/java/"+
						clazz.getPackage().getName().replace('.', '/')+"/package.properties"));
	}

	/**
	 * Manage a "hadooppreferences" file (package.properties)
	 * @param p the path of the package.properties file
	 */
	private HadoopPreferences(Path p){
		this.path = p;
	}


	/**
	 * Save a preference.
	 * 
	 * Save a preference that can be found
	 * later, @see HadoopPreferences#get(String, String)
	 * @param key the key necessary to retrieve the value
	 * @param value the value to store
	 */
	public void put(String key,String value){
		TreeMap<String,String> map = new TreeMap<String,String>();
	
		String[] keys = keys();
		for(String oldKey: keys){
			map.put(oldKey, get(oldKey,""));
		}
		map.put(key, value);
		write(map);
	}
	
	/**
	 * Remove a preference.
	 * 
	 * Remove a preference associated to a key
	 * @param key
	 */
	public void remove(String key){
		TreeMap<String,String> map = new TreeMap<String,String>();
		
		String[] keys = keys();
		for(String oldKey: keys){
			map.put(oldKey, get(oldKey,""));
		}
		
		map.remove(key);
		write(map);
	}
	

	/**
	 * Get a preference.
	 * 
	 * Get a preference.
	 * If no preferences are stored for
	 * this key are the file is not reachable,
	 * returns the default value.
	 * 
	 * @param key key needed to find the objectt
	 * @param defaultValue the default value
	 * @return the value
	 */
	public String get(String key, String defaultValue){
		try{
			//Read all the file and save the keys
			FileSystem fileSystem = NameNodeVar.getFS();

			if (!fileSystem.exists(path)) {
				logger.debug("File '"+path.getName()+"' does not exists");
				return defaultValue;
			}

			String value = defaultValue;

			FSDataInputStream in = fileSystem.open(path);
			LineReader reader = new LineReader(in);
			Text line = new Text();
			boolean found = false;
			while ( reader.readLine(line) != 0 && !found) {
				if(line.charAt(0) != '#'){
					int indexEq = line.toString().indexOf('=');
					String curKey = line.toString().substring(0, indexEq);
					if(curKey.equals(key)){
						value = line.toString().substring(indexEq+1);
						found = true;
					}
				}
			}

			in.close();
			fileSystem.close();
			return value;
		}catch(IOException e){
			logger.error("Cannot access to the preference file");
			logger.error(e.getMessage());
		}
		return defaultValue;
	}

	/**
	 * Get all the keys available within the package preference.
	 * 
	 * @return
	 */
	protected String[] keys(){
		try{
			//Read all the file and save the keys
			FileSystem fileSystem = FileSystem.get(conf);

			if (!fileSystem.exists(path)) {
				logger.debug("File '"+path.getName()+"' does not exists");
				return new String[]{};
			}

			List<String> keys = new LinkedList<String>();

			FSDataInputStream in = fileSystem.open(path);
			LineReader reader = new LineReader(in);
			Text line = new Text();
			while ( reader.readLine(line) != 0) {
				if(line.charAt(0) != '#')
					keys.add(line.toString().substring(0,line.toString().indexOf('=')));
			}

			in.close();
			fileSystem.close();
			return keys.toArray(new String[keys.size()]);
		}catch(IOException e){
			logger.error("Cannot access to the preference file");
			logger.error(e.getMessage());
		}
		return null;
	}

	/**
	 * Writes the key,value pairs into a file.
	 * 
	 * @param sorted_map
	 */
	private void write(TreeMap<String, String> sorted_map){

		if(logger.isDebugEnabled()){
			logger.debug("New values for preferences "+path.getName());
			Iterator<String> it = sorted_map.keySet().iterator();
			while( it.hasNext()) {
				logger.debug(it.next());
			}
			logger.debug("End list to write");
		}
		try{


			FileSystem fileSystem = FileSystem.get(conf);

			// Check if the file already exists
			if (fileSystem.exists(path)) {
				logger.debug("Remove file "+path.getName());
				fileSystem.delete(path,false);
			}

			// Create a new file and write data to it.
			FSDataOutputStream out = fileSystem.create(path);
			Iterator<String> it = sorted_map.keySet().iterator();
			while( it.hasNext()) {
				String key = it.next();
				String lineToWrite = key+"="+sorted_map.get(key)+"\n"; 
				out.write(lineToWrite.getBytes());
			}

			out.close();
			fileSystem.close();
		}catch(IOException e){
			logger.error("Cannot store the new preferences value(s)");
			logger.error(e.getMessage());
		}
	}

	public String absolutePath() {
		return path.toString();
	}

}