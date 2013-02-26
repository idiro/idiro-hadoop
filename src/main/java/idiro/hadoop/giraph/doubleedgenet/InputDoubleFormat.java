package idiro.hadoop.giraph.doubleedgenet;

import idiro.hadoop.giraph.IdiroVertexReader;
import idiro.hadoop.io.DoubleArrayWritable;

import java.io.IOException;

import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Read a double Graph.
 * Read a graph where:
 * vertex value: list of double
 * edge value: double
 * message: list of double
 *  
 * @author etienne
 *
 */
public class InputDoubleFormat extends
TextVertexInputFormat<Text, DoubleArrayWritable, DoubleWritable, DoubleArrayWritable>{

	@Override
	public VertexReader<Text, DoubleArrayWritable, DoubleWritable, DoubleArrayWritable> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new DoubleVertexReader(
	            textInputFormat.createRecordReader(split, context));
	}
	
	
	public static class DoubleVertexReader extends
	IdiroVertexReader<DoubleArrayWritable,DoubleWritable,DoubleArrayWritable>{

		public DoubleVertexReader(RecordReader<LongWritable, Text> arg0) {
			super(arg0);
		}

		@Override
		protected DoubleArrayWritable getNewDefaultVertexValue() {
			return new DoubleArrayWritable();
		}

		@Override
		protected DoubleWritable getNewEdge(double value) {
			return new DoubleWritable(value);
		}
		
	}
	
}
