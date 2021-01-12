package info7250.bigData.Project.Part3_1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map1 extends Mapper<LongWritable, Text, Text, Text>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		if(line.equals("userId,movieId,tag,timestamp")){
			return;
		}
		String[] data = line.split(",");
		Text movie = new Text();
		movie.set(data[1]);
		Text count = new Text();
		count.set("A");
		context.write(movie,count);
	}
	
}
