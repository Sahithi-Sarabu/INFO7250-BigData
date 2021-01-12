package info7250.bigData.Project.Part3_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, IntWritable>{

	@Override
	public int getPartition(CompositeKey key, IntWritable value, int partitionsCount) {
		return key.getTitle().hashCode() % partitionsCount;
	}
}
