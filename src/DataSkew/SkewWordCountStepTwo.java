package DataSkew;

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


public class SkewWordCountStepTwo {

	public static class SkewWordCountStepTwoMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		Text k=new Text();
		IntWritable v=new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split("-");
			String word=fields[0];
			int num = Integer.parseInt(fields[1].split("\t")[1]);
			
			k.set(word);
			v.set(num);
			
			context.write(k, v);
			
		}	
	}
	
	public static class SkewWordCountStepTwoReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count=0;
			for (IntWritable value : values) {
				count+=value.get();
				
			}
			
			context.write(key, new IntWritable(count));
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SkewWordCountStepTwo.class);
		
		job.setMapperClass(SkewWordCountStepTwoMapper.class);
		job.setReducerClass(SkewWordCountStepTwoReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\wordcount\\output"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\wordcount\\output2"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
}
