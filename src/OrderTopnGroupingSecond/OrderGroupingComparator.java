package OrderTopnGroupingSecond;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator{
	
	public OrderGroupingComparator() {
		// TODO Auto-generated constructor stub
		super(OrderBean.class,true);//true表示创建实例 
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		OrderBean o1=(OrderBean)a;
		OrderBean o2=(OrderBean)b;
		
		return o1.getOrderId().compareTo(o2.getOrderId());
	}

}
