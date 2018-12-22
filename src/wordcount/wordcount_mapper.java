package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * String ---> Text
 * int -->IntWritable
 * Long--->LongWritable
 * float-->FloatWritable
 * 
 */
public class wordcount_mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		String[] words = line.split(" ");
		
		for(String word:words) {
			context.write(new Text(word),new IntWritable(1));
		}
		
		
		
	}
}

