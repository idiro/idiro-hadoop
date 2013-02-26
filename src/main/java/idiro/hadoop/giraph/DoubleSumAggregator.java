package idiro.hadoop.giraph;

import org.apache.giraph.graph.Aggregator;
import org.apache.hadoop.io.DoubleWritable;

/**
 * Aggregator for summing up double values.
 * 
 * @author etienne
 * 
 */
public class DoubleSumAggregator 
implements Aggregator<DoubleWritable>{

	/** Internal sum */
	private double sum = 0;

	public void aggregate(double value) {
		sum += value;
	}

	@Override
	public void aggregate(DoubleWritable value) {
		sum += value.get();
	}

	@Override
	public void setAggregatedValue(DoubleWritable value) {
		sum = value.get();
	}

	@Override
	public DoubleWritable getAggregatedValue() {
		return new DoubleWritable(sum);
	}

	@Override
	public DoubleWritable createAggregatedValue() {
		return new DoubleWritable();
	}
}
