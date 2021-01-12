package info7250.bigData.Project.Part8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	     // Create a new Job
		Configuration config = new Configuration();
		Job job = Job.getInstance(config,"filterYearGenre");
	     job.setJarByClass(Driver.class);
	     job.setNumReduceTasks(0);
	     
	     Configuration mapConf1 = new Configuration(false);
	     ChainMapper.addMapper(job, Map.class, LongWritable.class, Text.class,
	         Text.class, CompositeValue.class,  mapConf1);
	         
	     Configuration mapConf2 = new Configuration(false);
	     ChainMapper.addMapper(job, Map2.class, Text.class, CompositeValue.class,
	         Text.class, NullWritable.class, mapConf2);
	     
	     FileInputFormat.addInputPath(job, new Path(args[0]));
	     FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(NullWritable.class);

	     System.exit(job.waitForCompletion(true) ? 0:1);
	}

}
