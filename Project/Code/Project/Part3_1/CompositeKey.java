package info7250.bigData.Project.Part3_1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{
	
	private String title;
	private int count;
	
	public CompositeKey(){
	}
	
	public CompositeKey(String title, int count) {
		this.title = title;
		this.count = count;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		title = in.readUTF();
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(title);
		out.writeInt(count);
	}

	@Override
	public int compareTo(CompositeKey o) {
		return o.count - count;
	}
}
