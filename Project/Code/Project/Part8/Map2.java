package info7250.bigData.Project.Part8;

import java.io.IOException;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<Text, CompositeValue, Text, NullWritable>{
	
	protected void map(Text key, CompositeValue value, Context context) throws IOException, InterruptedException{
		if(value.getTitle().contains("2011") && value.getGenre().contains("Action")) {
			Text movieName = new Text();
			movieName.set(value.getTitle());
			context.write(movieName, NullWritable.get());
		}else {
			return;
		}
	}
}
