/** 
 *  Copyright © 2016 Red Sqirl, Ltd. All rights reserved.
 *  Red Sqirl, Clarendon House, 34 Clarendon St., Dublin 2. Ireland
 *
 *  This file is part of Idiro Utility for Hadoop
 *
 *  User agrees that use of this software is governed by: 
 *  (1) the applicable user limitations and specified terms and conditions of 
 *      the license agreement which has been entered into with Red Sqirl; and 
 *  (2) the proprietary and restricted rights notices included in this software.
 *  
 *  WARNING: THE PROPRIETARY INFORMATION OF Idiro Utility for Hadoop IS PROTECTED BY IRISH AND 
 *  INTERNATIONAL LAW.  UNAUTHORISED REPRODUCTION, DISTRIBUTION OR ANY PORTION
 *  OF IT, MAY RESULT IN CIVIL AND/OR CRIMINAL PENALTIES.
 *  
 *  If you have received this software in error please contact Red Sqirl at 
 *  support@redsqirl.com
 */

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
