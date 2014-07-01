package com.idiro.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.jblas.DoubleMatrix;

/**
 * Implementation of Writable for a jblas Vector.
 * @author etienne
 *
 */
public class DoubleVectorWritable {

	private DoubleMatrix v;
	
	public DoubleVectorWritable(){
	}
	
	public DoubleVectorWritable(double[] values){
		this.v = new DoubleMatrix(values);
	}
	
	public DoubleVectorWritable(DoubleMatrix v){
		this.v = v;
	}
	
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		
		double[] values = new double[size];
		for (int i=0; i<values.length; i++){
			values[i] = in.readDouble();
		}
		v = new DoubleMatrix(values);
			
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(v.getLength());
		for (int i=0; i<v.getLength(); i++){
			out.writeDouble(v.get(i));
		}
	}

	/**
	 * @return the v
	 */
	public DoubleMatrix getV() {
		return v;
	}

	/**
	 * @param v the v to set
	 */
	public void setV(DoubleMatrix v) {
		this.v = v;
	}
	
}
