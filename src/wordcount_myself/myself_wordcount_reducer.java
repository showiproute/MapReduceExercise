package wordcount_myself;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myself_wordcount_reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Iterator<IntWritable> iterator = values.iterator();
		int counts=0;
		while(iterator.hasNext()) {
			IntWritable value = iterator.next();
			counts+=value.get();
			context.write(key, new IntWritable(counts));
		}
		
		
	}

}
