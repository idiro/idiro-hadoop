package idiro.hadoop.giraph.doubleedgenet;

import idiro.hadoop.giraph.IdiroVertexReader;
import idiro.hadoop.io.DoubleArrayWritable;
import idiro.hadoop.io.TextArray;

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
 * Create a graph with text messages.
 * 
 * Vertex id: text
 * Vertex value: double array
 * Edge value: double
 * Message value: text
 * 
 * @author etienne
 *
 */
public class InputDoubleFormatWithTextM extends
TextVertexInputFormat<Text, DoubleArrayWritable, DoubleWritable, TextArray>{

	@Override
	public VertexReader<Text, DoubleArrayWritable, DoubleWritable, TextArray> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new DoubleVertexReaderWithTextM(
	            textInputFormat.createRecordReader(split, context));
	}
	
	
	public static class DoubleVertexReaderWithTextM extends
	IdiroVertexReader<DoubleArrayWritable,DoubleWritable,TextArray>{

		public DoubleVertexReaderWithTextM(RecordReader<LongWritable, Text> arg0) {
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