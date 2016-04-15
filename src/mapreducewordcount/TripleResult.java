package mapreducewordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TripleResult implements Writable {
	int max;
	int min;
	int count;

	public void setMax(int max) {
		this.max = max;
	}
	public int getMax() {
		return max;
	}

	public int getMin() {
		return min;
	}

	public int getCount() {
		return count;
	}

	public void setMin(int min) {
		this.min = min;
	}
	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.max = arg0.readInt();
		this.min = arg0.readInt();
		this.count = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(max);
		arg0.writeInt(min);
		arg0.writeInt(count);
	}
	public String toString(){
		return new String(Integer.toString(max) + " " + Integer.toString(min) + " " + Integer.toString(count));
	}
}
