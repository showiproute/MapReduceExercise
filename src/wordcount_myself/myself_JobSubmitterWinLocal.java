package wordcount_myself;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import wordcount.JobSubmitterLinuxToYarn;
import wordcount.wordcount_mapper;
import wordcount.wordcount_reducer;

public class myself_JobSubmitterWinLocal {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		
		
	    Job job = Job.getInstance(conf);
	    
	    
	    job.setJarByClass(JobSubmitterLinuxToYarn.class);
	    job.setMapperClass(wordcount_mapper.class);
	    job.setReducerClass(wordcount_reducer.class);
	  
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	  
	    FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/wordcountmyself/input"));
	    FileOutputFormat.setOutputPath(job,new Path("f:/Linuxsource/mrdata/wordcountmyself/output"));
	    
	    job.setNumReduceTasks(3);
	    
	    boolean res = job.waitForCompletion(true);
	    System.exit(res?0:1);
	    
	}

}
