package MRCodingFive_GlobalOrder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import MRCodingFive_GlobalOrder.GlobalOrderStepOne.GlobalOrderStepOneMapper;
import MRCodingFive_GlobalOrder.GlobalOrderStepOne.GlobalOrderStepOneReducer;

public class GlobalOrderStepTwo {

	public static class GlobalOrderStepTwoMapper extends Mapper<LongWritable, Text,urlObject , NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, urlObject, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split("\t");
			String url=fields[0];
			int counts=Integer.parseInt(fields[1]);
			urlObject urlObject = new urlObject();
			urlObject.set(url, counts);
			
			context.write(urlObject, NullWritable.get());
			
		}
	
	}
	
	public static class GlobalOrderStepTwoReducer extends Reducer<urlObject, NullWritable, urlObject, NullWritable>{
		
		@Override
		protected void reduce(urlObject key, Iterable<NullWritable> values,
				Reducer<urlObject, NullWritable, urlObject, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			context.write(key, NullWritable.get());
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(GlobalOrderStepTwo.class);
		job.setMapperClass(GlobalOrderStepTwoMapper.class);
		job.setReducerClass(GlobalOrderStepTwoReducer.class);
	
		
		job.setMapOutputKeyClass(urlObject.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(urlObject.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\topn\\outputglobalstepone"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\topn\\outputglobalsteptwo"));
		
		boolean res= job.waitForCompletion(true);
	
		System.exit(res?0:-1);
	}
	
}
