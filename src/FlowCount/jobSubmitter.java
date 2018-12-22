package FlowCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;


public class jobSubmitter {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
	
		job.setJarByClass(jobSubmitter.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//设置参数：maptask在做数据分区时，用哪个分区逻辑类。如果不指定默认用hashpartitioner
		job.setPartitionerClass(ProvincePartitioner.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/flow/province_output"));
		
		//由于provincePartitioner可能会产生6中分区号，所以需要有6个reduce task来接收
		
		job.setNumReduceTasks(6);
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
}
