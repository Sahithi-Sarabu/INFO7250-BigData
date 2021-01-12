package info7250.bigData.Project.Part3_1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<LongWritable, Text, Text, Text>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		if(line.equals("movieId,title,genres")){
			return;
		}
		int firstIndex = line.indexOf(",");
		int lastIndex = line.lastIndexOf(",");
		String movieTitle = line.substring(firstIndex+1, lastIndex);
		String movieId = line.substring(0,firstIndex);
		Text movie = new Text();
		movie.set(movieId);
		Text title = new Text();
		title.set("B"+movieTitle);
		context.write(movie, title);
	}
	
}

