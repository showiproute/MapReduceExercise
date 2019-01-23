package FlowCount_myself;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCount_myselfReducer extends Reducer<Text, FlowBean_myself, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<FlowBean_myself> values,
			Reducer<Text, FlowBean_myself, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
			
		int upflowsum=0;
		int dflowsum=0;
		
		for(FlowBean_myself value:values) {
			upflowsum += value.getUpflow();
			dflowsum  +=value.getDflow();
		}
		
		context.write(new Text(key.toString()), new IntWritable(upflowsum+dflowsum));
	}
}
