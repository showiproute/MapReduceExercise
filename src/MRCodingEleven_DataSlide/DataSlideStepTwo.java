package MRCodingEleven_DataSlide;

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

public class DataSlideStepTwo {

	public static class DataSlideStepTwoMapper extends Mapper<LongWritable,Text, Text, IntWritable>{
		
		IntWritable v=new IntWritable();
		Text k=new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String[] fields = value.toString().split("\t");
			String word=fields[0].split("-")[0];
			v.set(Integer.parseInt(fields[1]));
			k.set(word);
			context.write(k, v);	
		}
	}
	
	public static class DataSlideStepTwoReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		IntWritable v=new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int total=0;
			for (IntWritable value : values) {
				total+=value.get();
			}
			v.set(total);
			context.write(key,v );
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(DataSlideStepTwo.class);
		
		job.setMapperClass(DataSlideStepTwoMapper.class);
		job.setReducerClass(DataSlideStepTwoReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\dataslide\\output"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\dataslide\\output2"));
	
		job.setNumReduceTasks(1);
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
	}
	
	
}
