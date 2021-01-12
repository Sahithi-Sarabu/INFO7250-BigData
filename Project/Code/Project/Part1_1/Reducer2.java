package info7250.bigData.Project.Part1_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<CompositeKey, NullWritable, Text, IntWritable>{
	
	protected void reduce(CompositeKey cmpk, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
		Text year = new Text();
		year.set(cmpk.getYear());
		IntWritable sum = new IntWritable(cmpk.getCount());
		context.write(year, sum);
	}
}
