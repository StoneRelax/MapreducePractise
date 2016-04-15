package mapreducewordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MeanTriple implements Writable{

	private int total;
	private int count;
	private float average ;
	
	public float getAverage() {
		return average;
	}
	public void setAverage(float average) {
		this.average = average;
	}
	public int getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.total = arg0.readInt();
		this.count = arg0.readInt();
		this.average = arg0.readFloat();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(total);
		arg0.writeInt(count);
		arg0.writeFloat(average);
	}
	public String toString(){
		return new String(total + " " + count + " " + average);
	}
}
