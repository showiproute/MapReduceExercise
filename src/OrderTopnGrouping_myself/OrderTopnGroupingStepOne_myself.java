package OrderTopnGrouping_myself;

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

public class OrderTopnGroupingStepOne_myself {
	
	public static class OrderTopnGroupingStepOne_myselfMapper extends Mapper<LongWritable, Text, OrderBean_myself, NullWritable>{
		
		OrderBean_myself orderbean = new OrderBean_myself();
		NullWritable outvalue=NullWritable.get();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, OrderBean_myself, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split(",");
			orderbean.set(fields[0], fields[1], fields[2], 
					Float.parseFloat(fields[3]),Integer.parseInt(fields[4]));
			
			context.write(orderbean,outvalue);
			
		}
	}
	
	public static class OrderTopnGroupingStepOne_myselfReducer extends Reducer<OrderBean_myself, NullWritable, OrderBean_myself, NullWritable>{
		
		@Override
		protected void reduce(OrderBean_myself key, Iterable<NullWritable> values,
				Reducer<OrderBean_myself, NullWritable, OrderBean_myself, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			
			int topn = context.getConfiguration().getInt("top.n", 5);
			int i=0;
			
			for(NullWritable v:values) {
				context.write(key, v);
				i++;
				if(i==topn) return;
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		conf.setInt("top.n", 3);
		
		Job job = Job.getInstance(conf);
		
		job.setPartitionerClass(OrderIdPartitioner_myself.class);
		job.setGroupingComparatorClass(OrderGroupingComparator_myself.class);
		job.setJarByClass(OrderTopnGroupingStepOne_myself.class);
		
		job.setMapperClass(OrderTopnGroupingStepOne_myselfMapper.class);
		job.setReducerClass(OrderTopnGroupingStepOne_myselfReducer.class);
		
		job.setMapOutputKeyClass(OrderBean_myself.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(OrderBean_myself.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(2);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/grouping_comparator_myself/input"));
		FileOutputFormat.setOutputPath(job,new Path("f:/Linuxsource/mrdata/grouping_comparator_myself/output"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
		
	}
	
	
}
