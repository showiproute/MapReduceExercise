package DataSkew;

import java.io.IOException;
import java.util.Random;

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




public class SkewWordCountStepOne {

	public static class SkewWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		Random random=new Random();
		Text k = new Text();
		IntWritable v=new IntWritable(1);
		int numReduceTasks=0;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			this.numReduceTasks = context.getNumReduceTasks();			
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String[] words = value.toString().split(" ");
			for (String w : words) {
				k.set(w+"-"+random.nextInt(numReduceTasks));
				context.write(k,v);
				
			}
		}
	}
	
	public static class SkewWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		
			int  count=0;
			for (IntWritable value : values) {
				count+=value.get();
			}
			context.write(key,new IntWritable(count));
		}
		
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SkewWordCountStepOne.class);
		
		job.setMapperClass(SkewWordCountMapper.class);
		job.setReducerClass(SkewWordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\wordcount\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\wordcount\\output"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
}
