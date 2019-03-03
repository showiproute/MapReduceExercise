package MRCodingTwelve_JsonDataAnalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ObjectPartitioner extends Partitioner<Text, MovieObject>{

	@Override
	public int getPartition(Text key, MovieObject value, int numPartitioners) {
		// TODO Auto-generated method stub
		return (key.hashCode() & Integer.MAX_VALUE )%numPartitioners;
	}
	

}
