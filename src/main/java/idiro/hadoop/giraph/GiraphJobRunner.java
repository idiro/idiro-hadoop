package idiro.hadoop.giraph;

import idiro.hadoop.HadoopVar;
import idiro.hadoop.db.hbase.HBaseStatement;
import idiro.hadoop.utils.TmpPath;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.MasterCompute;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * Class to launch a giraph job.
 * Sets the hbase configuration,
 * before launching the process.
 * 
 * @author etienne
 *
 */
public class GiraphJobRunner {

	static Logger logger = Logger.getLogger(GiraphJobRunner.class);

	@SuppressWarnings("rawtypes")
	public static boolean runGiraphJob(
			String hbase_manager,
			Configuration conf,
			Class<? extends TextVertexInputFormat> inputFormatClass,
			Class<? extends TextVertexOutputFormat> outputFormatClass,
			Class<? extends EdgeListVertex> vertexClass,
			Class<? extends VertexCombiner> combinerClass,
			Class<? extends WorkerContext> workerContextClass,
			Class<? extends MasterCompute> masterComputeClass,
			String hbaseTableNameOutput,
			Path inputJsonGraph,
			Map<String,String> hbNameWithType,
			Map<String,String>hbNameWithValueWanted,
			Iterable<Path> jars,
			int workerNb){
		
		boolean ok = true;
		try{
			Path outputDir = null;
			try {
				outputDir = TmpPath.create("giraphjob");
			} catch (Exception e) {
				logger.error("Fail to create temporary hdfs file");
				logger.error(e.getMessage());
			}
			
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outputDir)){
				logger.warn("Remove the previous content of the output path: "+outputDir.getName());
				fs.delete(outputDir, true);
			}

			logger.info("Jars to include in the cache: "+jars);
			Iterator<Path> itJars = jars.iterator();
			while(itJars.hasNext()){
				DistributedCache.addFileToClassPath(itJars.next(), conf,fs);
			}
			fs.close();

			logger.debug("zkQuorum: "+HadoopVar.get(HadoopVar.key_s_zkquorum));
			logger.debug("zkPort: "+HadoopVar.get(HadoopVar.key_s_zkport));

			conf.set("hbase.zookeeper.quorum", HadoopVar.get(HadoopVar.key_s_zkquorum));
			conf.set("hbase.zookeeper.property.clientPort", HadoopVar.get(HadoopVar.key_s_zkport));
			conf.set("tablenameOut",hbaseTableNameOutput);

			String[] ioVar = getConfVariables(hbase_manager, 
					hbaseTableNameOutput,
					hbNameWithType, 
					hbNameWithValueWanted);
			conf.set("valueWantedIn",ioVar[0]);
			conf.set("hbaseIdsWithTypeOut", ioVar[1]);
			conf.set("vertexclass", vertexClass.getCanonicalName());
			
			GiraphJob job = new GiraphJob(conf, vertexClass.getName());
			logger.info("zk: "+
			HadoopVar.get(HadoopVar.key_s_zkquorum).replaceAll(
					", ",":"+HadoopVar.get(HadoopVar.key_s_zkport)+",")+
					":"+HadoopVar.get(HadoopVar.key_s_zkport));
			job.setZooKeeperConfiguration(
					HadoopVar.get(HadoopVar.key_s_zkquorum).replaceAll(
							", ",":"+HadoopVar.get(HadoopVar.key_s_zkport)+",")+
							":"+HadoopVar.get(HadoopVar.key_s_zkport));

			job.setVertexClass(vertexClass);
			
			job.setVertexInputFormatClass(inputFormatClass);
			job.setVertexOutputFormatClass(outputFormatClass);
			FileInputFormat.addInputPath(job.getInternalJob(), inputJsonGraph);
			FileOutputFormat.setOutputPath(job.getInternalJob(), outputDir);
			if(combinerClass != null){
				job.setVertexCombinerClass(combinerClass);
			}
			if(workerContextClass != null){
				job.setWorkerContextClass(workerContextClass);
			}
			if(masterComputeClass != null){
				job.setMasterComputeClass(masterComputeClass);
			}
			job.setWorkerConfiguration(workerNb,
					workerNb,
					100.0f);

			if (! job.run(true)) {
				ok = false;
			}else{
				fs = FileSystem.get(conf);
				fs.delete(outputDir, true);
				fs.close();
			}
		} catch (IOException e) {
			logger.error("IOException in "+vertexClass.getName()+" process for seting up/running giraph job");
			logger.error(e.getMessage());
			ok = false;
		} catch (ClassNotFoundException e) {
			logger.error("ClassNotFoundException in "+vertexClass.getName()+" process for seting up/running giraph job");
			logger.error(e.getMessage());
			ok = false;
		} catch (InterruptedException e) {
			logger.error("InterruptedException in "+vertexClass.getName()+" process for seting up/running giraph job");
			logger.error(e.getMessage());
			ok = false;
		} catch (Exception e) {
			logger.error("Exception in "+vertexClass.getName()+" process for seting up/running giraph job");
			logger.error(e.getMessage());
			ok = false;
		}
		return ok;

	}

	/**
	 * Get a string array of size 2: 
	 * -0 the value that are to compute
	 * -1 in which id to store them
	 * note that the order of variables in the return strings is important
	 * @param hbNameWithType
	 * @param hbNameWithValueWanted
	 * @return
	 * @throws Exception 
	 */
	private static String[] getConfVariables(String hbase_manager,
			String tablename,
			Map<String,String> hbNameWithType,
			Map<String,String>hbNameWithValueWanted) throws Exception{

		if(hbNameWithType.size() != hbNameWithValueWanted.size()){
			throw new IOException("The number of value wanted '"+ 
					hbNameWithValueWanted.size()+
					"'and the number of field '"+
					hbNameWithType.size()+"does not match");
		}
		
		HBaseStatement hbSt = new HBaseStatement(hbase_manager);

		Iterator<String> hbNameIt = hbNameWithType.keySet().iterator();
		String inputToCompute = null;
		String outputToStore = null;
		if(hbNameIt.hasNext()){
			String hbName = hbNameIt.next();
			inputToCompute = hbNameWithValueWanted.get(hbName);
			outputToStore = hbSt.getHbaseField(tablename, hbName)
					+","+hbNameWithType.get(hbName);
		}
		while(hbNameIt.hasNext()){
			String hbName = hbNameIt.next();
			inputToCompute +=" "+ hbNameWithValueWanted.get(hbName);
			outputToStore +=" "+ hbSt.getHbaseField(tablename, hbName)
					+","+hbNameWithType.get(hbName);
		}
		logger.info("inputToCompute: '"+inputToCompute+"'");
		logger.info("outputToStore: '"+outputToStore+"'");
		return new String[]{inputToCompute,outputToStore};
	}
}
