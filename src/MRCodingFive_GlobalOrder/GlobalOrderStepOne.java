package MRCodingFive_GlobalOrder;

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

public class GlobalOrderStepOne {

	public static class GlobalOrderStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		Text k=null;
		IntWritable v=new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields= value.toString().split(" ");
			k=new Text(fields[1]);
			context.write(k, v);	
		}	
	}
	
	public static class GlobalOrderStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text url, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int total=0;
	
			for (IntWritable value : values) {
				total+=value.get();
			}
			
			context.write(url, new IntWritable(total));
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
	
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(GlobalOrderStepOne.class);
		job.setMapperClass(GlobalOrderStepOneMapper.class);
		job.setReducerClass(GlobalOrderStepOneReducer.class);
	
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\topn\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\topn\\outputglobalstepone"));
		
		boolean res= job.waitForCompletion(true);
	
		System.exit(res?0:-1);
	
	}
	
	
}
