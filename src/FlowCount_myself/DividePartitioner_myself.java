package FlowCount_myself;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DividePartitioner_myself extends Partitioner<Text, FlowBean_myself>{

	static HashMap<String,Integer> codemap=new HashMap<>();
	static {
		codemap.put("134", 0);
		codemap.put("135", 1);
		codemap.put("136", 2);
		codemap.put("137", 3);
		codemap.put("138", 4);
	}
	
	@Override
	public int getPartition(Text key, FlowBean_myself value, int numPartitions) {
		// TODO Auto-generated method stub
		Integer code = codemap.get(key.toString().substring(0, 3));

		return code==null?5:code;
	}

	
}
