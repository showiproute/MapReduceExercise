package MRCodingEight_GroupingComparator;

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



public class GroupingComparator {

	public static class GroupingComparatorMapper extends Mapper<LongWritable, Text, OrderObject, NullWritable>{
		OrderObject orderObject=new OrderObject();
		NullWritable v=NullWritable.get();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, OrderObject, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split(",");
			orderObject.set(fields[0], fields[1],fields[2],
					Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
		
			context.write(orderObject, v);
		}
	}
	
	public static class GroupingComparatorReducer extends Reducer<OrderObject, NullWritable, OrderObject, NullWritable>{
		@Override
		protected void reduce(OrderObject key, Iterable<NullWritable> values,
				Reducer<OrderObject, NullWritable, OrderObject, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			Configuration conf = context.getConfiguration();
			int topn = conf.getInt("order.top.n", 3);			
			/*
			 * 虽然reduce方法中的参数key只有一个，但是只要迭代器迭代一次，key中的值就会变
			 */
			int i=0;
			for (NullWritable value: values) {
				context.write(key, NullWritable.get());
				i++;
				if(i==topn) return;
			}
			
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		conf.setInt("order.top.n", 3);
		
		Job job= Job.getInstance(conf);
		job.setJarByClass(GroupingComparator.class);
		
		
		job.setPartitionerClass(OrderObjectPartitioner.class);
		job.setGroupingComparatorClass(OrderObjectGroupingComparator.class);
		
		job.setMapperClass(GroupingComparatorMapper.class);
		job.setReducerClass(GroupingComparatorReducer.class);
		
		job.setMapOutputKeyClass(OrderObject.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(OrderObject.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/ordergroup/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/ordergroup/ouput2"));
		
		job.setNumReduceTasks(2);
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
}
