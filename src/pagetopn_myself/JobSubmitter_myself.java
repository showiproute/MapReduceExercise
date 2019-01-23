package pagetopn_myself;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class JobSubmitter_myself {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		conf.setInt("top.n", 5);
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JobSubmitter_myself.class);
		
		job.setMapperClass(PagetopnMapper_myself.class);
		job.setReducerClass(PagetopnReducer_myself.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/pagetopnmyself/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/pagetopnmyself/output"));
	
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
}
