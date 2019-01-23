package pagetopnsortcount_myself;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageCountStep1_myself {

	public static class PageCountStep1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String line = value.toString();
			String[] split = line.split(" ");
			context.write(new Text(split[1]), new IntWritable(1));
			
		}
		
		
	}
	
	public static class PageCountStep1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int count=0;
			for(IntWritable value:values) {
				count+=value.get();
		
			}
			context.write(new Text(key.toString()), new IntWritable(count));	
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(PageCountStep1_myself.class);
		
		job.setMapperClass(PageCountStep1Mapper.class);
		job.setReducerClass(PageCountStep1Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/pagecountmyself/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/pagecountmyself/output"));
		
		job.setNumReduceTasks(3);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
		
	}
	
}
