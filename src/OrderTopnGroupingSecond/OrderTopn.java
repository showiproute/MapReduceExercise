package OrderTopnGroupingSecond;

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

public class OrderTopn{

	public static class OrderTopnMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
	
		OrderBean orderBean=new OrderBean();
		NullWritable v=NullWritable.get();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split(",");
			
			orderBean.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
	
			context.write(orderBean, v);
			
		}
	
	}
	
	public static class OrderTopnReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values,
				Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int i=0;
			for(NullWritable v:values) {
				
				context.write(key, v);
				
				if(++i==3) return;
				
			}
			
			
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(OrderTopn.class);
		
		job.setPartitionerClass(OrderIdPartitioner.class);
		job.setGroupingComparatorClass(OrderGroupingComparator.class);
		
		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);
		
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\ordergroup\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\ordergroup\\output2"));
		
		job.setNumReduceTasks(1);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
		
		
	}

}


