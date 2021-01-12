package info7250.bigData.Project.Part4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		if(line.equals("userId,movieId,tag,timestamp")){
			return;
		}
		String[] data = line.split(",");
		Text movie = new Text();
		movie.set(data[0]);
		context.write(movie, new IntWritable(1));
	}
	
}
