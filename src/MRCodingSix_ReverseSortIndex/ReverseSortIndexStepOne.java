package MRCodingSix_ReverseSortIndex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import MRCodingFive_GlobalOrder.GlobalOrderStepOne;
import MRCodingFive_GlobalOrder.GlobalOrderStepOne.GlobalOrderStepOneMapper;
import MRCodingFive_GlobalOrder.GlobalOrderStepOne.GlobalOrderStepOneReducer;

public class ReverseSortIndexStepOne {

	public static class ReverseSortIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		String fileName=null;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit split = (FileSplit) context.getInputSplit();
			fileName = split.getPath().getName();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] words = value.toString().split(" ");
			for (String word : words) {
				StringBuilder sb = new StringBuilder();
				sb.append(word).append("-").append(fileName);
				
				context.write(new Text(sb.toString()), new IntWritable(1));
			}
		
		}
	}
	
	public static class ReverseSortIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int total=0;
			for (IntWritable value : values) {
				total+=value.get();
			}
			
			context.write(key, new IntWritable(total));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ReverseSortIndexStepOne.class);
		job.setMapperClass(ReverseSortIndexMapper.class);
		job.setReducerClass(ReverseSortIndexReducer.class);
	
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\reversesortindex\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\reversesortindex\\output"));
		
		boolean res= job.waitForCompletion(true);
	
		System.exit(res?0:-1);
		
	}
	
}
