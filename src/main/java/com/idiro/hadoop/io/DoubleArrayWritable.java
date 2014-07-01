
package com.idiro.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Writable class for Double Array values.
 */
public class DoubleArrayWritable implements Writable {

	private static Random random = new Random(42);
	private double[] value = null;
	private Logger logger = Logger.getLogger(getClass());

	public DoubleArrayWritable() {
		value = new double[0];
	}
	
	public DoubleArrayWritable(int size){
		value = new double[size];
	}
	
	public DoubleArrayWritable(int size, double constant){
		value = new double[size];
		for (int i=0; i<value.length; i++)
			value[i] = constant;
	}

	public DoubleArrayWritable(double[] value) {
		this.value = value;
	}

	public DoubleArrayWritable(Double[] valueD){
		value = new double[valueD.length];
		for (int i=0; i<value.length; i++)
			value[i] = valueD[i];
	}
	
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		logger.info("DataInput Size: "+size);
		value = new double[size];
		for (int i=0; i<value.length; i++)
			value[i] = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(value.length);
		for (int i=0; i<value.length; i++)
			out.writeDouble(value[i]);
	}

	public String toString() {
		String ans = "";
		for (int i=0; i<value.length; i++) {
			ans += value[i];
			if (i < value.length-1)
				ans += " ";
		}
		return ans;
	}
	
	
	public static double[] parse(String doubleStr){
		String[] split = doubleStr.split(" ");
		double[] ans = new double[split.length];
		for(int i =0; i < split.length; ++i){
			ans[i] = Double.parseDouble(split[i]);
		}
		return ans;
	}
	
	public void set(double[] newVal){
		value = newVal;
	}
	

	/**
	 * Sets a = a + alpha*b.
	 * a and b must have the same lengths.
	 */
	public void add(double alpha, double b[]) {
		if(value.length!=b.length)
			throw new IllegalArgumentException("a.length!=b.length");
		for(int i=0;i<value.length;i++)
			value[i] += alpha*b[i];
	}

	public void add(double alpha, DoubleArrayWritable b) {
		add(alpha,b.getValue());
	}
	
	public void coef(double alpha){
		for(int i=0;i<value.length;i++)
			value[i] *= alpha;
	}
	
	
	/**
	 * Returns a new vector of length k having elements which are an
	 * i.i.d. sample from Uniform(0,1).
	 */
	public static double[] randu(int k) {
		double u[] = new double[k];
		for(int i=0;i<k;i++)
			u[i] = random.nextDouble() - 0.5;
		return u;
	}
	
	public static DoubleArrayWritable multiply(DoubleArrayWritable a, DoubleArrayWritable b){
		return DoubleArrayWritable.multiply(a.getValue(), b.getValue());
	}
	
	public static DoubleArrayWritable multiply(double a[],double b[]){
		if(a.length!=b.length)
			throw new IllegalArgumentException("a.length!=b.length");
		double[] prod = new double[a.length];
		for(int i=0;i<a.length;i++)
			prod[i] += a[i]*b[i];
		return new DoubleArrayWritable(prod);
	}

	/**
	 * Returns the dot product of a and b, which must have the same lengths.
	 */
	public static double dot(double a[], double b[]) {
		if(a.length!=b.length)
			throw new IllegalArgumentException("a.length!=b.length");
		double dotProd = 0;
		for(int i=0;i<a.length;i++)
			dotProd += a[i]*b[i];
		return dotProd;
	}
	
	public static double dot(DoubleArrayWritable a, DoubleArrayWritable b){
		return dot(a.getValue(),b.getValue());
	}


	/**
	 * @return the value
	 */
	public double[] getValue() {
		return value;
	}
	
	public int length(){
		return value.length;
	}

}

