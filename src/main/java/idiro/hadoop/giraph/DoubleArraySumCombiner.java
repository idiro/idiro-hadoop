package idiro.hadoop.giraph;

import idiro.hadoop.io.DoubleArrayWritable;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.giraph.graph.VertexCombiner;
import org.apache.hadoop.io.Text;

/**
 * Vertex Combiner to add arrays of same size.
 * 
 * @author etienne
 *
 */
public class DoubleArraySumCombiner 
extends VertexCombiner<Text,DoubleArrayWritable>{

	@Override
	public Iterable<DoubleArrayWritable> combine(Text vertexId,
			Iterable<DoubleArrayWritable> messages) throws IOException {
		
		LinkedList<DoubleArrayWritable> ld = new LinkedList<DoubleArrayWritable>();
		Iterator<DoubleArrayWritable> it = messages.iterator();
		double[] ans = new double[0];
		if(it.hasNext()){
		    double[] val = it.next().getValue();
			ans = new double[val.length];
			for(int i = 0; i < val.length;++i){
				ans[i] = val[i];
			}
		}
		while(it.hasNext()){
			double[] val = it.next().getValue();
			if(val.length!=ans.length){
				throw new IOException("The messages does not have all the same length, impossible to combine them");
			}
			for(int i = 0; i < val.length;++i){
				ans[i] += val[i];
			}
		}
		ld.add(new DoubleArrayWritable(ans));
		return ld;
	}

}
