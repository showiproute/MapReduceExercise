package Index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.iq80.leveldb.DB;

public class IndexStepTwo {

	public static class IndexStepTwoMapper extends Mapper<LongWritable, Text,Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split= value.toString().split("-");
			
			context.write(new Text(split[0]), new Text(split[1].replaceAll("\t", "-->")));
			
		}
	}
	public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text>{
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//stringbuffer是线程安全的，而stringbuilder是非线程安全的。在不涉及线程安全的场景下，stringbuilder更快
			StringBuilder sb = new StringBuilder();
			for(Text value:values) {
				sb.append(value.toString()).append("\t");
			}
			
			context.write(key,new Text(sb.toString()));
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepTwo.class);
		
		job.setMapperClass(IndexStepTwoMapper.class);
		job.setReducerClass(IndexStepTwoReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/index/output"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/index/indexoutput"));
		
		job.setNumReduceTasks(1);
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
}
