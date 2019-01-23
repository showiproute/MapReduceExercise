package IndexSecond;

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
	
	//产生<hello-文件名,1>
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			//inputSplit输入切片：用于描述每个MapTask所处理的数据任务范围
			
			//从输入切片信息中获取当前正在处理的一行数据所属的文件
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			String[] words = value.toString().split(" ");
			for (String w : words) {
				//将单词-文件名 作为key，1 作为value退出
				context.write(new Text(w+"-"+fileName), new IntWritable(1));
			
			}
			
			
		}
		
	}
	
	public static class IndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text text, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int count=0;
			for (IntWritable value: values) {
				count+=value.get();
				
			}
			
			context.write(text, new IntWritable(count));
		}
		
		
	}
	
	public static void main (String[] args) throws Exception{
		
		Configuration conf = new Configuration(); //默认只加载core-default.xml core-site.xml
		
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(IndexStepOne.class);
		
		job.setMapperClass(IndexStepOneMapper.class);
		job.setReducerClass(IndexStepOneReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\index\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\index\\output"));
		
		
		job.setNumReduceTasks(3);
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
	
}
