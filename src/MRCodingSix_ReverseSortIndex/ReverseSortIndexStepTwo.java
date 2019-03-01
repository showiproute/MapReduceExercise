package MRCodingSix_ReverseSortIndex;

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

import MRCodingSix_ReverseSortIndex.ReverseSortIndexStepOne.ReverseSortIndexMapper;
import MRCodingSix_ReverseSortIndex.ReverseSortIndexStepOne.ReverseSortIndexReducer;

public class ReverseSortIndexStepTwo {

	public static class ReverseSortIndexStepTwoMapper extends Mapper<LongWritable, Text, Text,Text >{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split("\t");
			String word=fields[0].split("-")[0];
			StringBuilder sb = new StringBuilder();
			sb.append(fields[0].split("-")[1]).append("-->").append(fields[1]);
			context.write(new Text(word), new Text(sb.toString()));
			
		}
	}
	
	public static class ReverseSortIndexStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value).append("\t");
			}
			
			context.write(key, new Text(sb.toString()));
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ReverseSortIndexStepTwo.class);
		job.setMapperClass(ReverseSortIndexStepTwoMapper.class);
		job.setReducerClass(ReverseSortIndexStepTwoReducer.class);
	
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\reversesortindex\\output"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\reversesortindex\\output2"));
		
		boolean res= job.waitForCompletion(true);
	
		System.exit(res?0:-1);
		
	}
}
