package info7250.bigData.Project.Part3_1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	     // Create a new Job
		Configuration config = new Configuration();
		Job job = Job.getInstance(config,"movieNameTagCount");
	    job.setJarByClass(Driver.class);
	     
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);

	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	    job.getConfiguration().set("join.type","inner");
	    job.setReducerClass(ReducerClass.class);

	    job.setOutputFormatClass(TextOutputFormat.class);
	    //FileOutputFormat.setOutputPath(job, new Path("/out2"));
	    TextOutputFormat.setOutputPath(job, new Path("/out2"));

	    if (!job.waitForCompletion(true)) {
	        System.exit(1);
	    }
	    
	    Job job2 = Job.getInstance(config,"movieNameTagCount-2");
	    job2.setJarByClass(Driver.class);
	    job2.setNumReduceTasks(1);
	    job2.setGroupingComparatorClass(NaturalGroupingKeyComparator.class);
	    job2.setSortComparatorClass(CompositeKeyComparator.class);
	    job2.setPartitionerClass(NaturalKeyPartitioner.class);
	    
	    FileInputFormat.addInputPath(job2, new Path("/out2"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	     
	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	     
	    job2.setMapOutputKeyClass(CompositeKey.class);
	    job2.setMapOutputValueClass(NullWritable.class);
	     
	    job2.setMapperClass(Map3.class);
	    job2.setReducerClass(Reducer2.class);
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);

	    System.exit(job2.waitForCompletion(true) ? 0:1);
	}

}
