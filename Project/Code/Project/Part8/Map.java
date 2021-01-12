package info7250.bigData.Project.Part8;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, CompositeValue>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		if(line.equals("movieId,title,genres")){
			return;
		}
		int firstIndex = line.indexOf(",");
		int lastIndex = line.lastIndexOf(",");
		String movieTitle = line.substring(firstIndex+1, lastIndex);
		String movieId = line.substring(0,firstIndex);
		String genres = line.substring(lastIndex+1);
		CompositeValue cv = new CompositeValue(movieTitle, genres);
		Text id = new Text();
		id.set(movieId);
		context.write(id, cv);
	}
	
}
