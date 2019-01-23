package wordcount_myself;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class myself_JobSubmitterWinToYarn {

	public static void main(String[] args) throws Exception{
		//在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
		System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		//1、设置Job运行时要访问的默认文件系统
		conf.set("fs.defaultFS", "hdfs://namenode:9000");
		//2、设置Job提交到哪去运行
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "namenode");
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		//3、如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		Job job = Job.getInstance(conf);
		
		job.setJar("f:/Linuxsource/wc3.jar");
		
		job.setMapperClass(myself_wordcount_mapper.class);
		job.setReducerClass(myself_wordcount_reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path output = new Path("/wordcountmyself/output");
		FileSystem fs = FileSystem.get(new URI("hdfs://namenode:9000"), conf, "root");
		if(fs.exists(output)) {
			fs.delete(output, true);
		}
		
		FileInputFormat.setInputPaths(job, new Path("/wordcountmyself/input"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcountmyself/output"));		
	
		job.setNumReduceTasks(2);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
		
	}
	
}
