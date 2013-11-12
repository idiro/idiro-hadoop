package idiro.hadoop.io;

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
