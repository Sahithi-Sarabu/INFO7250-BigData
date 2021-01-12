package info7250.bigData.Project.Part1_1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<LongWritable, Text, CompositeKey, NullWritable>{
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String tokens[] = value.toString().split("\\t");
		CompositeKey cpk = new CompositeKey(tokens[0], Integer.parseInt(tokens[1]));
		context.write(cpk, NullWritable.get());
	}
	
}
