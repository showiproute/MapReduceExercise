package MRCodingEight_GroupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderObjectGroupingComparator extends WritableComparator {

	public OrderObjectGroupingComparator() {
		// TODO Auto-generated constructor stub
		super(OrderObject.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		
		OrderObject o1=(OrderObject) a;
		OrderObject o2=(OrderObject) b; 
		
		return o1.getOrderId().compareTo(o2.getOrderId());
	}	
	
}
