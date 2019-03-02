package MRCodingEleven_DataSlide;

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

public class DataSlide {

	public static class DataSlideMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		int numReduceTasks=0;
		Random random=new Random();
		int nextInt=-1;
		Text k=new Text();
		IntWritable v=new IntWritable(1);
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
			for (String word: words) {
				StringBuilder sb = new StringBuilder();
				nextInt = random.nextInt(numReduceTasks);
				sb.append(word).append("-").append(nextInt);
				k.set(sb.toString());
				context.write(k,v);
			}
		}
	}
	
	public static class DataSlideReducer extends Reducer<Text,IntWritable, Text, IntWritable>{
		
		IntWritable v=new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int total=0;
			for (IntWritable value: values) {
				total+=value.get();
	
			}
			v.set(total);
			context.write(key,v );
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(DataSlide.class);
		
		job.setMapperClass(DataSlideMapper.class);
		job.setReducerClass(DataSlideReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\dataslide\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\dataslide\\output"));
		
		job.setNumReduceTasks(3);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
		
	}
}
