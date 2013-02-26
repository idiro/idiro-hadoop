package idiro.hadoop.giraph;

import org.apache.giraph.graph.Aggregator;
import org.apache.hadoop.io.LongWritable;

/**
 * Aggregator to sum long values.
 * 
 * @author etienne
 *
 */
public class LongSumAggregator
implements Aggregator<LongWritable>{

	/** Internal sum */
	private long sum = 0;

	public void aggregate(long value) {
		sum += value;
	}

	@Override
	public void aggregate(LongWritable value) {
		sum += value.get();
	}

	@Override
	public void setAggregatedValue(LongWritable value) {
		sum = value.get();
	}

	@Override
	public LongWritable getAggregatedValue() {
		return new LongWritable(sum);
	}

	@Override
	public LongWritable createAggregatedValue() {
		return new LongWritable();
	}
}