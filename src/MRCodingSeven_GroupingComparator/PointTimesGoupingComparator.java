package MRCodingSeven_GroupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PointTimesGoupingComparator extends WritableComparator {
	public PointTimesGoupingComparator() {
		// TODO Auto-generated constructor stub
		super(PointTimes.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		return 0;
	}

}
