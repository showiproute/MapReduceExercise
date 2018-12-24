package Index;

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

public class IndexStepOne {

	public static class IndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
		//map 产生<hello-文件名,1>
		/*
		 * Inputsplit输入切片:
		 * 		用于描述每个maptask所处理的数据的任务范围
		 * 
		 * 如果maptask读的是文件：
		 * 		划分范围应该用如下信息描述：
		 * 				文件路径、偏移量范围
		 * 
		 * 如果maptask度的是数据库的数据呢？
		 * 		划分任务范围应该用如下信息描述:
		 * 			库名、表名、行范围	 
		 * */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		
			//从输入切片信息中获取当前正在处理的一行数据所属的文件
			FileSplit inputSplit = (FileSplit)context.getInputSplit(); //返回inputsplit抽象类
			String filename = inputSplit.getPath().getName();
			
			String[] words = value.toString().split(" ");
			for(String word:words) {
				//将"单词-文件名"作为key,1作为value输出
				context.write(new Text(word+"-"+filename), new IntWritable(1));
			}
			
		}
		
	}
	
	public static class IndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int count=0;
			for(IntWritable value:values) {
				count+=value.get();
			}
			context.write(new Text(key.toString()), new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepOne.class);
		
		job.setMapperClass(IndexStepOneMapper.class);
		job.setReducerClass(IndexStepOneReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/index/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/index/output"));
		
		job.setNumReduceTasks(3);
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
	
}
