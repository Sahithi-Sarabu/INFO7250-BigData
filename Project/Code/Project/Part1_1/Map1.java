package info7250.bigData.Project.Part1_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		if(line.equals("movieId,title,genres")){
			return;
		}
		String[] data = line.split(",");
		
		int startIndex = data[1].lastIndexOf("(");
		if(startIndex < 0 || !Character.isDigit(data[1].charAt(startIndex +1)))
			return;
		String yearValue = new String();
		yearValue = data[1].substring(startIndex+1, startIndex+5);
		Text year = new Text();
		year.set(yearValue);
		context.write(year, new IntWritable(1));
	}
	
}
