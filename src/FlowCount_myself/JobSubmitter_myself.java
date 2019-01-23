package FlowCount_myself;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitter_myself {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JobSubmitter_myself.class);
		
		job.setMapperClass(FlowCount_myselfmapper.class);
		job.setReducerClass(FlowCount_myselfReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean_myself.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/flowcountmyself/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/flowcountmyself/outputpartitioner"));
		
		job.setPartitionerClass(DividePartitioner_myself.class);
		job.setNumReduceTasks(6);
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
	
	}
}
