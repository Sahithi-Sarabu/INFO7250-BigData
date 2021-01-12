package info7250.bigData.Project.Part3_1;

import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	
	public CompositeKeyComparator(){
		super(CompositeKey.class, true);
	}
	
	public int compare(CompositeKey a, CompositeKey b){
		return b.compareTo(a);
	}
}
