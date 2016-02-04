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
import org.apache.hadoop.io.Writable;

/**
 * Implementation of Writable for a long array.
 * 
 * @author etienne
 *
 */
public class LongArrayWritable implements Writable{

	private long[] value;
	public LongArrayWritable(){
		value = new long[0];
	}
	
	public LongArrayWritable(long[] vals){
		value = vals;
	}
	
	
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		value = new long[size];
		for (int i=0; i<value.length; i++)
			value[i] = in.readLong();
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(value.length);
		for (int i=0; i<value.length; i++)
			out.writeLong(value[i]);
		
	}
	
	public long[] get(){
		return value;
	}
	
	public void set(long[] value){
		this.value = value;
	}
	
	public int size(){
		return value.length;
	}

	
	
}
