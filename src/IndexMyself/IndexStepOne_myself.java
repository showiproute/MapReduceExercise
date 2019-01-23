package IndexMyself;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexStepOne_myself{
	public static class IndexStepOne_myselfMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit input=(FileSplit)context.getInputSplit();
			String file = input.getPath().getName();
			String[] words = value.toString().split(" ");
	
			for(String word:words) {
				context.write(new Text(word+"-"+file), new IntWritable(1));
			}
			
			
		}
	}
	
	
	public static class IndexStepOne_myselfReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int count=0;
			for(IntWritable value:values) {
				count+=value.get();
			}
			
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepOne_myself.class);
		
		job.setMapperClass(IndexStepOne_myselfMapper.class);
		job.setReducerClass(IndexStepOne_myselfReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/indexmyself/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/indexmyself/output"));
		
		job.setNumReduceTasks(3);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
		
	}
	
	
}