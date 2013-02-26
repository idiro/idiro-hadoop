package idiro.hadoop.giraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * General class to load a vertex into a graph.
 * 
 * We suppose here that a vertex id is always a
 * Text.
 * The edge value can be anything but has to be
 * initialised from a double value.
 * 
 * The file to be read in has to be in the format:
 * [ vertexId, [[vertexId, double] [vertexId, double]...]
 * 
 * @author etienne
 *
 * @param <V> Vertex value
 * @param <E> Edge Value
 * @param <M> Message Value
 */
public abstract class IdiroVertexReader<V extends Writable,E extends Writable, M extends Writable> extends
TextVertexInputFormat.TextVertexReader<Text, V, E, M> {

	
	MutableVertex<Text, V, E, M> vertex;
	
	public IdiroVertexReader(RecordReader<LongWritable, Text> arg0) {
		super(arg0);
	}
	
	@Override
	public BasicVertex<Text, V, E, M> getCurrentVertex()
			throws IOException, InterruptedException {	
		return vertex;
		
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean nextVertex() throws IOException, InterruptedException {

		
		
		if (!getRecordReader().nextKeyValue()) {
			vertex = null;
			return false;
		}
		
		Text line = getRecordReader().getCurrentValue();
		try {
			try {
				vertex = (MutableVertex<Text, V, E, M>) 
						Class.forName(getContext().getConfiguration().get("vertexclass")).newInstance();
				
			} catch (Exception e) {
				throw new IOException("The vertex class: '"+
						getContext().getConfiguration().get("vertexclass")+
								"' cannot be instancied: "+e.getMessage());
			}
			
			JSONArray jsonVertex = new JSONArray(line.toString());
			Text id = new Text(jsonVertex.getString(0));
			V value = getNewDefaultVertexValue();
			
			Map<Text,E> edges = new HashMap<Text,E>();
			JSONArray jsonEdgeArray = jsonVertex.getJSONArray(1);
			for (int i = 0; i < jsonEdgeArray.length(); ++i) {
				JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
				edges.put(new Text(jsonEdge.getString(0)), getNewEdge(jsonEdge.getDouble(1)));
			}
			vertex.initialize(id, value, edges, new LinkedList<M>());
			
		} catch (JSONException e) {
			throw new IllegalArgumentException(
					"next: Couldn't get vertex from line " + line, e);
		} 
		
		return true;
	}

	protected abstract V getNewDefaultVertexValue();
	
	protected abstract E getNewEdge(double value);

}
