package info7250.bigData.Project.Part1_1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{
	
	private String year;
	private int count;
	
	public CompositeKey(){
	}
	
	public CompositeKey(String year, int count) {
		this.year = year;
		this.count = count;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readUTF();
		count = in.readInt();
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(year);
		out.writeInt(count);
	}


	@Override
	public int compareTo(CompositeKey o) {
		return o.count - count;
	}

	
}
