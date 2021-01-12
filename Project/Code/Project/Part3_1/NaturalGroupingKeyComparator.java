package info7250.bigData.Project.Part3_1;

import org.apache.hadoop.io.WritableComparator;

public class NaturalGroupingKeyComparator extends WritableComparator {
	
	
	public NaturalGroupingKeyComparator(){
		super(CompositeKey.class, true);
	}
	
	public int compare(CompositeKey a, CompositeKey b){
		return a.getTitle().compareTo(b.getTitle());
	}

}
