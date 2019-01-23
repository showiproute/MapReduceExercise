package pagetopnsortcount_myself;

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

public class PageCountStep2_myself {

	public static class PageCountStep2Mapper extends Mapper<LongWritable, Text, PageCount_myself, NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, PageCount_myself, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] split = line.split("\t");
			
			context.write(new PageCount_myself(split[0], Integer.parseInt(split[1])), NullWritable.get());
			
		}
		
	}
	
	public static class PageCountStep2Reducer extends Reducer<PageCount_myself, NullWritable, PageCount_myself, NullWritable>{
		
		@Override
		protected void reduce(PageCount_myself key, Iterable<NullWritable> value,
				Reducer<PageCount_myself, NullWritable, PageCount_myself, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
	
		job.setJarByClass(PageCountStep2_myself.class);
		
		job.setMapperClass(PageCountStep2Mapper.class);
		job.setReducerClass(PageCountStep2Reducer.class);
		
		job.setMapOutputKeyClass(PageCount_myself.class);
		job.setMapOutputValueClass(NullWritable.class);
	
		job.setOutputKeyClass(PageCount_myself.class);
		job.setOutputValueClass(NullWritable.class);
	
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/pagecountmyself/output"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/pagecountmyself/sortoutput"));
		
		job.setNumReduceTasks(1);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
		
	}
	
}
