package info7250.bigData.Project.Part1_1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
		Job job = Job.getInstance(config,"movieCountYear-1");
	    job.setJarByClass(Driver.class);
	     
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path("/out"));
	     
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	     
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	     
	    job.setMapperClass(Map1.class);
	    job.setReducerClass(ReducerClass.class);
	    
	    if (!job.waitForCompletion(true)) {
	        System.exit(1);
	    }
	    
	    Job job2 = Job.getInstance(config,"movieCountYear-2");
	    job2.setJarByClass(Driver.class);
	    job2.setNumReduceTasks(1);
	    job2.setGroupingComparatorClass(NaturalGroupingKeyComparator.class);
	    job2.setSortComparatorClass(CompositeKeyComparator.class);
	    job2.setPartitionerClass(NaturalKeyPartitioner.class);
	    
	    FileInputFormat.addInputPath(job2, new Path("/out"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	     
	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	     
	    job2.setMapOutputKeyClass(CompositeKey.class);
	    job2.setMapOutputValueClass(NullWritable.class);
	     
	    job2.setMapperClass(Map2.class);
	    job2.setReducerClass(Reducer2.class);
	     
	     job2.setOutputKeyClass(Text.class);
	     job2.setOutputValueClass(IntWritable.class);

	     System.exit(job2.waitForCompletion(true) ? 0:1);
	}

}
