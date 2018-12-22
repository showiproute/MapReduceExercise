package pagetopn;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class JobSubmitter {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
//		conf.setInt("top.n", 3);
//		conf.setInt("top.n", Integer.parseInt(args[0]));
		/*
		 * 通过属性配置文件加参数
		 */
//		Properties props = new Properties();
//		props.load(JobSubmitter.class.getClassLoader().getResourceAsStream("topn.properties"));
//		conf.setInt("top.n", Integer.parseInt(props.getProperty("top.n")));
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(JobSubmitter.class);
		
		job.setMapperClass(PageTopnMapper.class);
		job.setReducerClass(PageTopnReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/topn/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/topn/output"));
		
		job.setNumReduceTasks(1);
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
}
