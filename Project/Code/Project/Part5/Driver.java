package info7250.bigData.Project.Part5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	     // Create a new Job
		Configuration config = new Configuration();
		Job job = Job.getInstance(config,"ratingCount");
	     job.setJarByClass(Driver.class);
	     
	     FileInputFormat.addInputPath(job, new Path(args[0]));
	     FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     
	     job.setInputFormatClass(TextInputFormat.class);
	     job.setOutputFormatClass(TextOutputFormat.class);
	     
	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(IntWritable.class);
	     
	     job.setMapperClass(Map.class);
	     job.setReducerClass(ReducerClass.class);
	     
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(IntWritable.class);

	     System.exit(job.waitForCompletion(true) ? 0:1);
	}

}
