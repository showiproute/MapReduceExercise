package OrderTopnGroupIng;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitioner) {
		// TODO Auto-generated method stub
		
		//根据订单中的orderID分给同一个reduce
		return (key.getOrderID().hashCode() & Integer.MAX_VALUE) % numPartitioner;
	}
	
	
	

}
