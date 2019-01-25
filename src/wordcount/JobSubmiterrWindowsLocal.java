package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobSubmiterrWindowsLocal {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","file:///");
		conf.set("mapreduce.framework.name", "local");
		
	    Job job = Job.getInstance(conf);
	    
	    
	    job.setJarByClass(JobSubmitterLinuxToYarn.class);
	    job.setMapperClass(wordcount_mapper.class);
	    job.setReducerClass(wordcount_reducer.class);
	  
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    //设置maptask端的局部聚合逻辑类
	    job.setCombinerClass(CombinerTask.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    

	    
	  
	    FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/wordcount/input"));
	    FileOutputFormat.setOutputPath(job,new Path("f:/Linuxsource/mrdata/wordcount/output2"));
	    
	    job.setNumReduceTasks(3);
	    
	    boolean res = job.waitForCompletion(true);
	    System.exit(res?0:1);
	    
	    
	}
}
