package OrderTopnGrouping_myself;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator_myself extends WritableComparator{
	
	public OrderGroupingComparator_myself() {
		
		super(OrderBean_myself.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		OrderBean_myself o1=(OrderBean_myself)a;
		OrderBean_myself o2=(OrderBean_myself)b;
		
		return o1.getOrderId().compareTo(o2.getOrderId());
		
	}
	
	
}
