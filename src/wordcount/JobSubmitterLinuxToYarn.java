package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 如果要在hadoop集群的某台机器上启动这个job提交客户端的话
 * conf里面就不需要指定fs.defaultFS mapreduce.framework.name
 * 
 * 因为在集群机器上用hadoop jar xx.jar wordcount.JobSubmitter2 命令来启动客户端main方法时
 * hadoop jar这个命令会将所在机器上的hadoop安装目录中的jar包和配置文件加入到运行时的ClassPath中
 * 那么我们的客户端，Main方法中的new Configuration 就会加载class path中的配置文件
 * 自然就有 fs.defaultFS 和mapreduce.framework.name 和yarn.resource.manager.name这些参数配置
 */

public class JobSubmitterLinuxToYarn {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//没指定默认文件系统
		//没指定mapreduce-job 提交到哪运行
		
		
	    Job job = Job.getInstance(conf);
	    
	    
	    job.setJarByClass(JobSubmitterLinuxToYarn.class);
	    job.setMapperClass(wordcount_mapper.class);
	    job.setReducerClass(wordcount_reducer.class);
	  
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	  
	    FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
	    FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));
	    
	    job.setNumReduceTasks(3);
	    
	    boolean res = job.waitForCompletion(true);
	    System.exit(res?0:1);
	    
	    
	}
}
