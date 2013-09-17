package idiro.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Implementation of Writable for a text array.
 * @author etienne
 *
 */
public class TextArray implements Writable{
	
	private Text[] value;
	
	public TextArray(){
		value = new Text[0];
	}
	
	public TextArray(Text[] vals){
		value = vals;
	}
	
	public TextArray(String[] vals){
		value = new Text[vals.length];
		for(int i=0;i<vals.length;++i){
			value[i] = new Text(vals[i]);
		}
	}
	
	
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		value = new Text[size];
		for (int i=0; i<value.length; i++){
			value[i] = new Text();
			value[i].readFields(in);
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(value.length);
		for (int i=0; i<value.length; i++)
			value[i].write(out);
	}
	
	public Text[] get(){
		return value;
	}
	
	public void set(Text[] value){
		this.value = value;
	}
	
}
