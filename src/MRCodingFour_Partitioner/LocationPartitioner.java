package MRCodingFour_Partitioner;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LocationPartitioner  extends Partitioner<Text, FlowObject>{

	static HashMap<String, Integer> codeMap=new HashMap<>();
	
	static {
		codeMap.put("130", 0);
		codeMap.put("131", 1);
		codeMap.put("132", 2);
		codeMap.put("133", 3);
		codeMap.put("134", 4);
	}
	
	@Override
	public int getPartition(Text key, FlowObject value, int numPartitions) {
		// TODO Auto-generated method stub
	
		String substring = key.toString().substring(0, 3);
		Integer integer = codeMap.get(substring);
		return integer==null?5:integer;
	}
	

}
