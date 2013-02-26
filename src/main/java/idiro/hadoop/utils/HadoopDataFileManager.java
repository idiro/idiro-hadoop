package idiro.hadoop.utils;

import idiro.check.DataFileChecker;
import idiro.hadoop.NameNodeVar;
import idiro.hadoop.checker.HdfsFileChecker;
import idiro.utils.DataFileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;


/**
 * Manages data file in hdfs.
 * 
 * Create a data file from a map reduce job.
 * 
 * Format a data file to be an input of a map reduce job
 * (no header, no quotes).
 * 
 * Validate a data file, for that the file is copied locally
 * and checked @see idiro.utils.DataFileUtils.
 * 
 * @author etienne
 *
 */
public class HadoopDataFileManager{

	private static Logger logger = Logger.getLogger(HadoopDataFileManager.class);

	protected static String conf_del_in = "delimiter_in";
	protected static String conf_del_out = "delimiter_out";
	
	/**
	 * Create a data file from a map reduce directory.
	 * @param dir the map reduce directory
	 * @param dataFile the output file
	 * @param delimiterIn the map reduce delimiter
	 * @param delimiterOut the data file delimiter
	 * @param quotes the quotes to add for each field
	 * @param header the header to add
	 * 
	 * @return true if process OK
	 */
	public static boolean fromMapRedDirToDataFile(Path dir, Path dataFile, 
			char delimiterIn,char delimiterOut, 
			Collection<String>quotes,Collection<String> header){
		//We expect that in is a mapRedDir and out a file
		logger.debug("Write directory "+dir.getName()+" to file "+dataFile.getName());
		logger.debug("header :"+header);
		logger.debug("quotes:"+quotes);

		boolean ok = true;


		FileSystem fs = null;
		try {
			fs = FileSystem.get(NameNodeVar.getConf());			
		} catch (IOException e2) {
			logger.error(e2.getMessage());
			logger.error("Cannot initialize hadoop filesystem");
			return false;
		}

		HdfsFileChecker fChIn = new HdfsFileChecker(dir);
		HdfsFileChecker fChOut = new HdfsFileChecker(dataFile);

		if(!fChIn.isDirectory()){
			logger.error(fChIn.getFilename()+" is not a mapRedDir or does not exist");
			fChIn.close();
			fChOut.close();
			return false;
		}

		if(fChOut.exists()){
			logger.warn(fChOut.getFilename()+" already exists, it will be removed");
			try {
				fs.delete(dataFile,true);
			} catch (IOException e) {
				logger.error("Fail to delete the file "+fChOut.getFilename());
				fChIn.close();
				fChOut.close();
				return false;
			}
		}


		FileStatus[] filesTable = null;
		try {
			filesTable = fs.listStatus(dir, new PathFilter(){

				@Override
				public boolean accept(Path arg0) {
					return !arg0.getName().startsWith(".");
				}

			});			
		} catch (IOException e2) {
			logger.error(e2.getMessage());
			logger.error("Cannot initialize hadoop filesystem");
			return false;
		}

		FSDataOutputStream  bw = null;
		FSDataInputStream  br = null;
		int i = 0;

		try {
			bw = fs.create(dataFile);
			if(header != null && !header.isEmpty()){
				Iterator<String> it = header.iterator();
				String headerLine = it.next();
				while(it.hasNext()){
					headerLine += delimiterOut + it.next();
				}
				bw.write((headerLine+"\n").getBytes());
			}
			for(; i < filesTable.length;++i){
				logger.debug("read the file"+filesTable[i].getPath().getName());
				br = fs.open(filesTable[i].getPath());
				//Read File Line By Line
				LineReader reader = new LineReader(br);
				Text line = new Text();
				while ( reader.readLine(line) != 0) {
					bw.write((DataFileUtils.addQuotesToLine(
							line.toString().replace(delimiterIn, delimiterOut), 
							quotes, 
							delimiterOut)+
							"\n").getBytes()
							);
				}
				br.close();
			}
			bw.close();
			fs.close();


		} catch (FileNotFoundException e1) {
			logger.error(e1.getCause()+" "+e1.getMessage());
			logger.error("Fail to read "+filesTable[i].getPath().getName());
			ok = false;
		} catch (IOException e1) {
			logger.error("Error writting, reading on the filesystem from the mapRedDir"+
					fChIn.getFilename()+" to the file "+fChOut.getFilename());
			ok = false;
		}

		fChIn.close();
		fChOut.close();
		return ok;
	}

	/**
	 * Create a data file from a map reduce directory, no header.
	 * @param dir the map reduce directory
	 * @param dataFile the output file
	 * @param delimiterIn the map reduce delimiter
	 * @param delimiterOut the data file delimiter
	 * @param quotes the quotes to add for each field
	 * 
	 * @return true if process OK
	 */
	public static boolean fromMapRedDirToDataFile(Path dir, Path dataFile, char delimiterIn, char delimiterOut, Collection<String> quotes){
		return fromMapRedDirToDataFile(dir, dataFile, delimiterIn,delimiterOut,quotes,null);
	}

	
	/**
	 * Create a map reduce directory from a database
	 * @param dataFile the input file
	 * @param mapRedDir the output directory
	 * @param delimiterIn the delimiter of the data file
	 * @param delimiterOut the delimiter of the map reduce directory
	 * @param header true if there is a header in the input file
	 * @return true if process OK
	 */
	public static boolean fromDataFileToMapRedDir(
			Path dataFile, Path mapRedDir, 
			char delimiterIn, char delimiterOut, boolean header){
		//TODO BUG here the delimiter in can be in the field due to the "clean line" method
		boolean ok = true;
		logger.debug("read datafile and clean up it for ISM...");

		//Have to copy across from the local file system to a HDFS mapRedDir
		org.apache.hadoop.fs.FileSystem dfs;
		try {
			dfs = NameNodeVar.getFS();

			if(!dfs.isFile(dataFile)){
				logger.error(dataFile+" is a mapRedDir");
				return false;
			}

			if(dfs.exists(mapRedDir)){
				logger.warn("Override "+mapRedDir);
				dfs.delete(mapRedDir,true);
			}

			Path fileTo = new Path(mapRedDir,"0000");

			FSDataOutputStream  bw = dfs.create(fileTo);
			FSDataInputStream  br = dfs.open(dataFile);


			logger.debug("read the file "+dataFile.getName());
			//Read File Line By Line
			LineReader reader = new LineReader(br);
			Text line = new Text();
			if(header){
				reader.readLine(line);
			}
			while ( reader.readLine(line) != 0) {
				bw.write((DataFileUtils.getCleanLine(line.toString(),delimiterIn,delimiterOut)+"\n").getBytes());
			}
			br.close();
			bw.close();
			dfs.close();			

		} catch (IOException e1) {
			logger.error("Fail to clean file"+dataFile.getName());
			logger.error(e1.getMessage());
			ok = false;
		}
		return ok;
	}

	/**
	 * Validate a data file in hdfs.
	 * 
	 * The method copy the file into the local file system to be checked.
	 * @param features the features contained in order with type
	 * @param file the input file
	 * @param delimiter the delimiter
	 * @param header true if there is a header
	 * @return true if process OK
	 */
	public static boolean validate(Map<String,String> features, Path file, char delimiter, boolean header){
		List<String> types = new LinkedList<String>();
		Iterator<String> it = features.keySet().iterator();
		while(it.hasNext()){
			types.add(features.get(it.next()));
		}

		return validate(types,file,delimiter,header);
	}

	/**
	 * Validate a data file in hdfs.
	 * 
	 * The method copy the file into the local file system to be checked.
	 * @param types the type of the features in order
	 * @param file the input file
	 * @param delimiter the delimiter
	 * @param header true if there is a header
	 * @return true if process OK
	 */
	public static boolean validate(Collection<String> types, Path file, char delimiter,boolean header){
		boolean ok = true;

		FileSystem dfs ;
		try {
			dfs = NameNodeVar.getFS();
		} catch (IOException e) {
			logger.error(e.getMessage());
			logger.error("Cannot initialize hadoop filesystem");
			return false;
		}

		File locF = null;
		try {
			locF = File.createTempFile("idiro", file.getName());
		} catch (IOException e1) {
			logger.error("Fail to create temporary local file");
			ok = false;
		}
		if(ok){
			if(locF.exists()){
				locF.delete();
			}
			Path locFile = new Path(locF.getAbsolutePath());

			try {
				dfs.copyToLocalFile(file, locFile);
				dfs.close();
			} catch (IOException e) {
				logger.error("Cannot copy file "+file.getName() +" to "+locF.getAbsolutePath());
				logger.error(e.getMessage());
				ok = false;
			}
		}

		if(ok)
			ok = new DataFileChecker(locF,delimiter).validate(types,header);

		locF.delete();

		return ok;
	}

}


