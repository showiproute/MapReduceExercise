package MRCodingEight_GroupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderObjectPartitioner extends Partitioner<OrderObject, NullWritable>{

	@Override
	public int getPartition(OrderObject key, NullWritable value, int numPartitioners) {
		// TODO Auto-generated method stub
		return (key.getOrderId().hashCode() & Integer.MAX_VALUE )%numPartitioners;
	}
	

}
