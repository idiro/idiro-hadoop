package idiro.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.jblas.DoubleMatrix;

/**
 * Implementation of Writable for a jblas Symmetric matrix.
 * @author etienne
 *
 */
public class SymMatrixWritable implements Writable{

	private DoubleMatrix m;
	
	public SymMatrixWritable(double[][] values){
		this.m = new DoubleMatrix(values);
	}
	
	public SymMatrixWritable(DoubleMatrix m){
		this.m = m;
	}
	
	public SymMatrixWritable() {
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		
		double[][] values = new double[size][size];
		for (int i=0; i<values.length; i++){
			values[i][i] = in.readDouble();
			for (int j=i+1; j<values.length; j++){
				values[i][j] = values[j][i] = in.readDouble();
			}
		}
		m = new DoubleMatrix(values);
			
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(m.getColumns());
		for (int i=0; i<m.getColumns(); i++){
			out.writeDouble(m.get(i,i));
			for (int j=i+1; j<m.getColumns(); j++){
				out.writeDouble(m.get(i,j));
			}
		}
	}

	/**
	 * @return the m
	 */
	public DoubleMatrix getM() {
		return m;
	}

	/**
	 * @param m the m to set
	 */
	public void setM(DoubleMatrix m) {
		this.m = m;
	}
		

}
