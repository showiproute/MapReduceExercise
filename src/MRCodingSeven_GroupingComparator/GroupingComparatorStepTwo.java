package MRCodingSeven_GroupingComparator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GroupingComparatorStepTwo {

	public static class GroupingComparatorStepTwoMapper extends Mapper<LongWritable, Text, PointTimes, NullWritable>{
		
		NullWritable v=NullWritable.get();
		PointTimes pointTimes=new PointTimes();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, PointTimes, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split("\t");			
			pointTimes.set(fields[0], Integer.parseInt(fields[1]));
			
			context.write(pointTimes, v);
			
		}
	}
	
	public static class GroupingComparatorStepTwoReducer extends Reducer<PointTimes, NullWritable, PointTimes, NullWritable>{
		
		NullWritable v=NullWritable.get();
		@Override
		protected void reduce(PointTimes key, Iterable<NullWritable> values,
				Reducer<PointTimes, NullWritable, PointTimes, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			int topn = conf.getInt("top.n.value", 3);
			int i=0;
			for (NullWritable value:values) {
				context.write(key, v);
				i++;
				if(i==topn) return;
			}
			
		}
		
	}
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(GroupingComparatorStepTwo.class);
		
		job.setGroupingComparatorClass(PointTimesGoupingComparator.class);
		job.setMapperClass(GroupingComparatorStepTwoMapper.class);
		job.setReducerClass(GroupingComparatorStepTwoReducer.class);
		
		job.setMapOutputKeyClass(PointTimes.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(PointTimes.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\groupcomparator\\output"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\groupcomparator\\output2"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
		
	}
	
}
