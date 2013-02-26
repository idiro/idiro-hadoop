package idiro.hadoop.giraph;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;

/**
 * Class to write a vertex.
 * 
 * At the end of a giraph job, a
 * TextVertexWriter class is called
 * to write the results.
 * This class has to be used with 
 * GiraphJobRunner (@see idiro.hadoop.giraph.GiraphJobRunner).
 * 
 * This abstract class write into an hbase table.
 * The row is the vertex id.
 * 
 * @author etienne
 *
 * @param <V> Vertex Valued
 * @param <E> Edge Value
 */
public abstract class IdiroGiraphVertexWriter<V extends Writable,E extends Writable> 
extends TextVertexWriter<Text, V, E> {

	private static HTable tableOut;

	/**
	 * String[]: 0 column family, 1 column, 2 type
	 */
	private static List<String[]> valOutput = new LinkedList<String[]>();

	public IdiroGiraphVertexWriter(RecordWriter<Text, Text> lineRecordWriter, Configuration conf) throws IOException{
		super(lineRecordWriter);
		tableOut = new HTable(conf, conf.get("tablenameOut"));
		String[] hbaseIdsWithType = conf.get("hbaseIdsWithTypeOut").split(" ");
		for(String hbaseIdWithType: hbaseIdsWithType){
			String[] valToAdd = hbaseIdWithType.split("[:,]");
			if(valToAdd.length != 3){
				throw new IOException("The field 'hbaseIdsWithTypeOut' have to contain '$cf:$var,$type [...]'");
			}
			valOutput.add(valToAdd);

		}
		if(valOutput.isEmpty()){
			throw new IOException("The giraph process has nothing to write, original in string: "+
					conf.get("hbaseIdsWithTypeOut"));
		}
	}

	@Override
	public void writeVertex(
			BasicVertex<Text, V, E, ?> vertex)
					throws IOException, InterruptedException {
		Put put = new Put(vertex.getVertexId().getBytes());
		put = putVal(put, valOutput,vertex.getVertexValue());
		tableOut.put(put);
	}

	public abstract Put putVal(Put put, Collection<String[]> hbaseIdsWType, V vertexValue)throws IOException;
}