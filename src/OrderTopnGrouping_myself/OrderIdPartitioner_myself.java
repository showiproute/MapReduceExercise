package OrderTopnGrouping_myself;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner_myself extends Partitioner<OrderBean_myself, NullWritable>{

	@Override
	public int getPartition(OrderBean_myself key, NullWritable values, int partitionerNum) {
		// TODO Auto-generated method stub
		
		
		return (key.getOrderId().hashCode()&Integer.MAX_VALUE)%partitionerNum;
	}
	
	

}
