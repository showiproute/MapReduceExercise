package OrderTopn;

import java.io.IOException;
import java.util.ArrayList;

import java.util.Collections;

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

public class OrderTopnStepOne {

	public static class OrderTopnStepOneMapper extends Mapper<LongWritable, Text, Text, OrderBean>{
		OrderBean orderBean = new OrderBean();
		Text k=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, OrderBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split(",");
			
			orderBean.set(fields[0], fields[1], fields[2],Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
			k.set(fields[0]);
			
			//从这里交给maptask的kv对象，会被maptask序列化后存储的，所以不用担心覆盖问题
			context.write(k, orderBean);
			
		}
		
	}
	
	public static class OrderTopnStepOneReducer extends Reducer<Text, OrderBean, OrderBean,NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<OrderBean> values,
				Reducer<Text, OrderBean, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int topn = context.getConfiguration().getInt("order.top.n", 5);
			
			ArrayList<OrderBean> beanlist = new ArrayList<>();
			
			//reduce task提供的values迭代器，每次迭代返回给我们的都是同一个对象，只是set了不同的值
			for (OrderBean orderBean : values) {
				//构造一个新的对象，来存储每次迭代出来的值
				OrderBean newBean = new OrderBean();
				newBean.set(orderBean.getOrderID(), orderBean.getUserID(), orderBean.getPdtName(), orderBean.getPrice(), orderBean.getNumber());
			
				beanlist.add(newBean);
			}
			
			//对beanlist中的orderbean对象排序<按照总金额大小倒序，如果总金额相同，比商品名称
			Collections.sort(beanlist);
			for(int i=0;i<topn;i++) {
				context.write(beanlist.get(i), NullWritable.get());
			}
			
		}
		
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.setInt("order.top.n", 3);
		
		Job job= Job.getInstance(conf);
		job.setJarByClass(OrderTopnStepOne.class);
		
		job.setMapperClass(OrderTopnStepOneMapper.class);
		job.setReducerClass(OrderTopnStepOneReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderBean.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/ordertopnstepone/input"));
		FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/ordertopnstepone/ouput"));
		
		job.setNumReduceTasks(1);
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}

}
