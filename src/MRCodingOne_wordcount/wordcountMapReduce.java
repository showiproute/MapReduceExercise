package MRCodingOne_wordcount;

import java.io.IOException;
import java.util.Iterator;

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

public class wordcountMapReduce {

	public static class wordcountMapReduceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String[] words = value.toString().split(" ");
			for (String word : words) {
				context.write(new Text(word), new IntWritable(1));
				
			}		
		}
		
	}
	
	public static class wordcountMapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text word, Iterable<IntWritable> iter,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int total=0;
			Iterator<IntWritable> iterator = iter.iterator();
			while(iterator.hasNext()) {
				
				IntWritable count = iterator.next();
				total+=count.get();
			}
			context.write(word, new IntWritable(total));
			
		}
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(wordcountMapReduce.class);
		
		job.setMapperClass(wordcountMapReduceMapper.class);
		job.setReducerClass(wordcountMapReduceReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job,new Path("F:\\Linuxsource\\mrdata\\wordcount\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\wordcount\\output"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
}
