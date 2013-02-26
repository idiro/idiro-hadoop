package idiro.hadoop.utils;

import idiro.hadoop.NameNodeVar;

import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Utility methods for creating temporary files.
 * 
 * Creates temporary paths in a specific folder on hdfs.
 * At the moment of writing the folder is "/tmp/idiro/"
 * 
 * @author etienne
 *
 */
public class TmpPath {

	private static final Logger logger = Logger.getLogger(TmpPath.class);
	public static final String tmpPath = "/tmp/idiro/";
	public static final int length = 15;


	/**
	 * Create a temporary path that does not exists.
	 * 
	 * Create a temporary path that does not exists
	 * and with a random name
	 * @return
	 * @throws Exception
	 */
	public static Path create() throws Exception{
		return create("","");
	}

	/**
	 * Create a temporary path that does not exists.
	 * 
	 * Create a temporary path that does not exists
	 * and with a random name with a suffix.
	 * 
	 * @param suffix
	 * @return
	 * @throws Exception
	 */
	public static Path create(String suffix) throws Exception{
		return create("",suffix);
	}

	/**
	 * Create a temporary path that does not exists.
	 * 
	 * Create a temporary path that does not exists
	 * and with a random name with a prefix and a suffix.
	 * @param prefix
	 * @param suffix
	 * @return
	 * @throws Exception
	 */
	public static Path create(String prefix, String suffix) throws Exception{
		
		StringBuilder strb = new StringBuilder(tmpPath);
		if(prefix != null && !prefix.isEmpty()){
			strb.append(prefix).append('_');
		}

		String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		Random rnd = new Random();
		char[] new_name = new char[length];
		new_name[0] =  characters.charAt(rnd.nextInt(characters.length()-10));
		for (int i = 1; i < length; i++){
			new_name[i] =  characters.charAt(rnd.nextInt(characters.length()));
		}
		strb.append(new_name);
		if(suffix != null && !suffix.isEmpty()){
			strb.append('_').append(suffix);
		}

		Path ans = new Path(strb.toString());
		FileSystem dfs = NameNodeVar.getFS();
		if(dfs.exists(ans)){
			dfs.close();
			logger.warn("Path "+strb+" already exists, try again");
			ans = create(suffix);
		}else{
			logger.info("New tmp folder created: "+strb.toString());
		}
		return ans;
	}

}
