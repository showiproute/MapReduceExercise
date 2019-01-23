package FlowCount_myself;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCount_myselfmapper extends Mapper<LongWritable, Text, Text, FlowBean_myself>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean_myself>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] split = line.split("\t");
		
		String phone=split[1];
		int upflow=Integer.parseInt(split[split.length-3]);
		int dflow=Integer.parseInt(split[split.length-2]);
		
	
		context.write(new Text(phone), new FlowBean_myself(phone,upflow,dflow));
		
	
	}

}
