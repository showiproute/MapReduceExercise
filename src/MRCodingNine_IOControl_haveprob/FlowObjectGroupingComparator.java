package MRCodingNine_IOControl_haveprob;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlowObjectGroupingComparator extends WritableComparator{
	public FlowObjectGroupingComparator() {
		// TODO Auto-generated constructor stub
		super(FlowObject.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
}
