package MRCodingNine_IOControl_haveprob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SelfDesinTypeStepTwo {

	public static class SelfDesinTypeStepTwoMapper extends Mapper<Text, FlowObject, Text, FlowObject>{
		
		@Override
		protected void map(Text key, FlowObject value, Mapper<Text, FlowObject, Text, FlowObject>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, value);
			
		}
	}
	
	public static class SelfDesinTypeStepTwoReducer extends Reducer<Text, FlowObject, Text, FlowObject>{
		@Override
		protected void reduce(Text key, Iterable<FlowObject> values,
				Reducer<Text, FlowObject, Text, FlowObject>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			Configuration conf = context.getConfiguration();
			int topn= conf.getInt("top.n.value", 3);
			int i=0;
			for (FlowObject value : values) {
				context.write(key,value);
				i++;
				if(i==topn) return;
			}
		
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		conf.setInt("top.n.value", 3);
		
		Job job = Job.getInstance(conf);		
		
		job.setJarByClass(SelfDesinTypeStepTwo.class);
		job.setMapperClass(SelfDesinTypeStepTwoMapper.class);
		job.setReducerClass(SelfDesinTypeStepTwoReducer.class);
		
//		job.setGroupingComparatorClass(FlowObjectGroupingComparator.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowObject.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowObject.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);

		SequenceFileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/flow/output5"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/flow/output6"));
		

		job.setNumReduceTasks(1);
		
		boolean res= job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
	
}
