package MRCodingFour_Partitioner;

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

public class SelfDesinType {

	
	public static class SelfDesinTypeMapper extends Mapper<LongWritable, Text, Text,FlowObject >{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowObject>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split("\t");
			String phone=fields[1];
			FlowObject flowObject = new FlowObject();
			flowObject.set(phone, Integer.parseInt(fields[fields.length-3]),Integer.parseInt(fields[fields.length-2]));
		
			context.write(new Text(phone), flowObject);
		}
	}
	
	public static class SelfDesinTypeReducer extends Reducer<Text, FlowObject, Text, FlowObject>{
		@Override
		protected void reduce(Text phone, Iterable<FlowObject> iter,
				Reducer<Text, FlowObject, Text, FlowObject>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int uFlow=0;
			int dFlow=0;
			
			for (FlowObject flowObject : iter) {
				uFlow+=flowObject.getuFlow();
				dFlow+=flowObject.getdFlow();
			}
			FlowObject flowObject = new FlowObject();
			flowObject.set(phone.toString(),uFlow, dFlow);
			context.write(phone, flowObject);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SelfDesinType.class);
		job.setMapperClass(SelfDesinTypeMapper.class);
		job.setReducerClass(SelfDesinTypeReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowObject.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowObject.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/flow/output3"));
		
		//设置参数：maptask在做数据分区时，用哪个分区逻辑类。如果不指定默认用hashpartitioner
		job.setPartitionerClass(LocationPartitioner.class);
		
		//由于provincePartitioner可能会产生6中分区号，所以需要有6个reduce task来接收
		job.setNumReduceTasks(6);
		
		boolean res= job.waitForCompletion(true);
		
		System.exit(res?0:-1);
	}
	
}
