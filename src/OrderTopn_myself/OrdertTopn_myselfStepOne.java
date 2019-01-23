package OrderTopn_myself;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class OrdertTopn_myselfStepOne {

	public static class OdertTopn_myselfStepOneMapper extends Mapper<LongWritable, Text,Text , OrderBean_myself>{
		OrderBean_myself obm=new OrderBean_myself();
		Text k=new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, OrderBean_myself>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			String[] fields = value.toString().split(",");
			obm.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
			k.set(fields[0]);
			
			context.write(k,obm);
			
		}
		
	}
	
	public static class OderTopn_myselfStepOneReducer extends Reducer<Text, OrderBean_myself, Text, OrderBean_myself>{
		
		@Override
		protected void reduce(Text key, Iterable<OrderBean_myself> values,
				Reducer<Text, OrderBean_myself, Text, OrderBean_myself>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			ArrayList<OrderBean_myself> arrayList=new ArrayList<>();
			
			int topn = context.getConfiguration().getInt("top.n", 5);

			for (OrderBean_myself value : values) {
				OrderBean_myself orderBean_myself = new OrderBean_myself();
				orderBean_myself.set(value.getOrderID(), value.getUsername(), value.getBrandname(), value.getPrice(), value.getNum());
				arrayList.add(orderBean_myself);
			}
			Collections.sort(arrayList);
			
			for(int i=0;i<topn;i++) {
				context.write(key, arrayList.get(i));
			}
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.setInt("top.n", 3);
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(OrdertTopn_myselfStepOne.class);
		
		job.setMapperClass(OdertTopn_myselfStepOneMapper.class);
		job.setReducerClass(OderTopn_myselfStepOneReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderBean_myself.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(OrderBean_myself.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/ordertopn_myself/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/ordertopn_myself/output"));
		
		job.setNumReduceTasks(1);
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
	
}
