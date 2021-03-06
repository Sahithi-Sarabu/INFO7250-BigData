package info7250.bigData.Project.Part2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	IntWritable result = new IntWritable();
	
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		for(IntWritable value : values){
			count += value.get();
		}
		result.set(count);
		Text genre = new Text();
		genre.set("genre " + key);
		context.write(genre, result);
	}
}

