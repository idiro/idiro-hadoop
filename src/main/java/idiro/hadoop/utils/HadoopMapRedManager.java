package idiro.hadoop.utils;

import idiro.hadoop.NameNodeVar;
import idiro.hadoop.checker.HdfsFileChecker;
import idiro.utils.DataFileUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * Manages map reduce directory in hdfs.
 * 
 * A map-reduce directory contains column delimited files
 * without header and quotes.
 * 
 * @author etienne
 *
 */
public class HadoopMapRedManager {

	private static Logger logger = Logger.getLogger(HadoopMapRedManager.class);

	public static final String conf_delimiter = "delimiter";
	public static final String conf_types = "types";

	public static boolean validate(Path mapredDir, char delimiter, Map<String,String> features){
		return validate(mapredDir,delimiter,features.values());
	}


	public static boolean validate(Path mapredDir, char delimiter, Collection<String> features){

		boolean ok = true;
		Configuration conf = getConf(delimiter,features);
		String jobName = "validateMapRed";
		Job job = null;
		Path outputDir = null;

		if(features == null || features.size() == 0){
			logger.error("No features to check");
			ok = false;
		}else{
			HdfsFileChecker ch = new HdfsFileChecker(mapredDir);
			if(!ch.isDirectory()){
				logger.error("'"+mapredDir+"' does not exist or is not a directory");
				ok = false;
			}
			ch.close();
		}

		if(ok){			
			try {
				outputDir = TmpPath.create(jobName);
			} catch (Exception e) {
				logger.error("Fail to create temporary hdfs file");
				ok = false;
			}
		}

		if(ok){

			try{

				job = new Job(conf,jobName);
				job.setJarByClass(HadoopMapRedManager.class);
				job.setJarByClass(DataFileUtils.class);


				job.setMapperClass(ValidationMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);


				FileInputFormat.addInputPath(job, mapredDir);
				FileOutputFormat.setOutputPath(job, outputDir);

			}catch(IOException e){
				logger.error("Fail to initialise the job for validate '"+mapredDir+"'");
				ok = false;
			}

		}

		if(ok){
			// Run the job
			logger.debug("Running " + jobName + " job...");
			try {
				ok = job.waitForCompletion(true);
				if(!ok){
					logger.error("The map reduce job for validation did not run successfully");
				}
			} catch (ClassNotFoundException e) {
				logger.error("One or more Classes not found");
				logger.error(e.getMessage());
				ok = false;
			} catch (InterruptedException e) {
				logger.error("Job Interrupted");
				logger.error(e.getMessage());
				ok = false;
			} catch (IOException e) {
				logger.error("The map-reduce directory is not valid");
				logger.error(e.getMessage());
				ok = false;
			}
		}

		if(outputDir != null){

			FileSystem dfs;
			try {
				dfs = NameNodeVar.getFS();
				if(dfs.exists(outputDir)){
					dfs.delete(outputDir,true);
				}
				dfs.close();
			} catch (IOException e) {
				logger.warn("Fail to remove '"+outputDir+"'");
			}
		}

		return ok;
	}

	/**
	 * Get the configuration file
	 * @param delimiter the delimiter
	 * @param types the types in order.
	 * @return
	 */
	protected static Configuration getConf(char delimiter, Collection<String> typesList) {
		Configuration conf = NameNodeVar.getConf();
		conf.set(conf_delimiter, Character.toString(delimiter));

		Iterator<String> it = typesList.iterator();
		StringBuilder types = new StringBuilder(it.next());
		while(it.hasNext()){
			types.append('|').append(it.next());
		}
		conf.set(conf_types,types.toString());
		return conf;
	}

	public static class ValidationMapper
	extends Mapper<Object, Text, Text, Text>{

		public static boolean init = false; 
		public static List<String> types;
		public static char delimiter;

		@Override
		public void setup(Context context) throws IOException,InterruptedException{
			super.setup(context);
			Configuration conf = context.getConfiguration();
			if(!init){
				delimiter = conf.get(HadoopMapRedManager.conf_delimiter).charAt(0);
				String[] typesA = conf.get(conf_types).split("\\|");
				List<String> typesL = new LinkedList<String>();
				for(int i = 0;i<typesA.length;++i){
					typesL.add(typesA[i]);
				}
				types = typesL;
				logger.debug("Delimiter: "+delimiter);
				logger.debug("types "+types);
				init = true;
			}
		}

		/**
		 * Function which reads the raw file
		 * 
		 * Two kind of file can be found: churn file and cdr
		 * Each file have columns separate by '|'.
		 */
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String line = value.toString();
			String cleanLine = DataFileUtils.getCleanLine(line, delimiter, delimiter);
			if(!line.equals(cleanLine)){
				throw new IOException("The line '"+line+
						"' has quotes or is not cleaned, expected to be '"+
						cleanLine+"'");
			}else if(!DataFileUtils.validateLine(types, line, delimiter)){
				throw new IOException("In the line '"+line+
						"' the types does not match their declaration");
			}
		}
	}
}
