package info7250.bigData.Project.Part5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		if(line.equals("userId,movieId,rating,timestamp")){
			return;
		}
		String[] data = line.split(",");
		Text rating = new Text();
		rating.set(data[2]);
		context.write(rating, new IntWritable(1));
	}
	
}
