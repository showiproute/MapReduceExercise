package wordcount_myself;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class myself_wordcount_mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String values = value.toString();
		String[] words = values.split(" ");
		for(String word:words) {
			context.write(new Text(word), new IntWritable(1));
		}
		
		
		
	}
}
