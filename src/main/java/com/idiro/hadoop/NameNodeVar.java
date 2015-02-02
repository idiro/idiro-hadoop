package com.idiro.hadoop;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;

/**
 * Stores the hadoop namenode URI in memory.
 * 
 * The only indispensable variable for reading what is on HDFS is the namenode
 * URI. We retrieve all the other variables from here.
 * 
 * @author etienne
 *
 */
public class NameNodeVar {

	private String jobtracker;

	private String nameNodeURI;

	private static NameNodeVar instance = new NameNodeVar();

	private NameNodeVar() {
	}

	public static boolean isInit() {
		return instance.nameNodeURI != null && !instance.nameNodeURI.isEmpty();
	}

	public static boolean isTrackerInit() {
		return instance.jobtracker != null && !instance.jobtracker.isEmpty();
	}

	public static String get() {
		return instance.nameNodeURI;
	}

	public static void setJobTracker(String jobtracker) {
		instance.jobtracker = jobtracker;
	}

	public static String getJobTracker() {
		return instance.jobtracker;
	}

	public static void set(String nameNodeURI) {
		instance.nameNodeURI = nameNodeURI;
	}

	/**
	 * Get a basic configuration file with the right fs.default
	 * 
	 * @return
	 */
	public static Configuration getConf() {
		Configuration conf = new Configuration();
		if (isInit()) {
			conf.set("fs.default.name", NameNodeVar.get());
		}
		return conf;
	}

	/**
	 * Get the HDFS
	 * 
	 * @return
	 * @throws IOException
	 */
	public static FileSystem getFS() throws IOException {
		if (isInit()) {
			return FileSystem.get(NameNodeVar.getConf());
		}
		return null;
	}

	public static int getNbSlaves() {
		int slaves = 0;
		if (isTrackerInit()) {
			try {
				JobClient theJobClient = new JobClient(new InetSocketAddress(
						instance.getJobTracker().substring(0,
								instance.getJobTracker().indexOf(":")),
						Integer.valueOf(
								instance.getJobTracker()
										.substring(
												instance.getJobTracker()
														.indexOf(":") + 1))
								.intValue()), instance.getConf());
				
				slaves = theJobClient.getClusterStatus().getTaskTrackers();
			} catch (IOException e) {
				System.out.println(e);
			}

		}

		return slaves;
	}
}
