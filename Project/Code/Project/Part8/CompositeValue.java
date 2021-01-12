package info7250.bigData.Project.Part8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CompositeValue implements Writable{
	
	private String title;
	private String genre;
	
	public CompositeValue(){		
	}
	
	public CompositeValue(String title, String genre) {
		this.title = title;
		this.genre = genre;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		title = in.readUTF();
		genre = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(title);
		out.writeUTF(genre);
	}

}
