package idiro.hadoop.giraph.doubleedgenet;

import idiro.hadoop.giraph.IdiroGiraphVertexWriter;
import idiro.hadoop.io.DoubleArrayWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Writes the vertex values (double or integer) into HBase.
 * 
 * 
 * @author etienne
 *
 */
public class OutputDoubleFormat
extends TextVertexOutputFormat<Text,DoubleArrayWritable, DoubleWritable>{

	@Override
	public VertexWriter<Text, DoubleArrayWritable, DoubleWritable> createVertexWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		RecordWriter<Text, Text> recordWriter =
				textOutputFormat.getRecordWriter(context);
		return new DoubleVertexWriter(recordWriter,context.getConfiguration());
	}

	public static class DoubleVertexWriter extends
	IdiroGiraphVertexWriter<DoubleArrayWritable, DoubleWritable> {

		public DoubleVertexWriter(RecordWriter<Text, Text> lineRecordWriter, Configuration conf) throws IOException{
			super(lineRecordWriter,conf);
		}

		@Override
		public Put putVal(Put put, Collection<String[]> hbaseIdsWType,
				DoubleArrayWritable vertexValue) throws IOException{

			double[] values = vertexValue.getValue();
			if(hbaseIdsWType.size() != values.length){
				throw new IOException("The number of value expected '"+hbaseIdsWType.size()+
						"' is different than the number of value received '"+values.length+"'");
			}
			
			int i = 0;
			Iterator<String[]> hbaseIdIt = hbaseIdsWType.iterator();
			while(hbaseIdIt.hasNext()){
				String[] hbaseId = hbaseIdIt.next();
				String type = hbaseId[2];
				if(type.equalsIgnoreCase("int") ||
						type.equalsIgnoreCase("boolean")){
					put.add(Bytes.toBytes(hbaseId[0]), 
							Bytes.toBytes(hbaseId[1]), 
							Bytes.toBytes(Integer.toString((int)values[i])));
				}else{
					put.add(Bytes.toBytes(hbaseId[0]), 
							Bytes.toBytes(hbaseId[1]), 
							Bytes.toBytes(Double.toString(values[i])));
				}

				++i;
			}
			return put;
		}
	}
}
