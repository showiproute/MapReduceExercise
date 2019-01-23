package pagetopn_myself;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PagetopnMapper_myself extends Mapper<LongWritable,Text , Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] split = line.split(" ");
		
		context.write(new Text(split[1]), new IntWritable(1));
		
	}
}
