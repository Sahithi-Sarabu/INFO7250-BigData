package info7250.bigData.Project.Part2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		if(line.equals("movieId,title,genres")){
			return;
		}
		int genreStartIndex = line.lastIndexOf(",");
		String genreString = line.substring(genreStartIndex+1);
		if(genreString.equals("(no genres listed)")) {
			return;
		}
		String[] genreList = genreString.split("\\|");
		Text genre = new Text();
		for(int i=0; i< genreList.length; i++) {
			genre.set(genreList[i]);
			context.write(genre, new IntWritable(1));
		}
	}
	
}
