package MRCodingTen_JoinAlgorithm;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinAlogrithm {

	public static class JoinAlogrithmMapper extends Mapper<LongWritable, Text, Text,BeanObject >{
		private String fileName=null;
		BeanObject beanObject=new BeanObject();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, BeanObject>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			fileName=inputSplit.getPath().getName();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, BeanObject>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(fileName.startsWith("order")) {
				String[] fields = value.toString().split(",");
				beanObject.set(fields[0], fields[1], "null", -1, "null","order");
				context.write(new Text(fields[1]), beanObject);
			}else if(fileName.startsWith("user")) {
				String[] fields = value.toString().split(",");
				beanObject.set("null", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3],"user");
				context.write(new Text(fields[0]), beanObject);
			}else {
				return ;
			}
		}
	}
	
	public static class JoinAlogrithmReducer extends Reducer<Text, BeanObject, BeanObject, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<BeanObject> values,
				Reducer<Text, BeanObject, BeanObject,NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ArrayList<BeanObject> arrayList = new ArrayList<>();
			BeanObject beanObject=null;
			
			for (BeanObject value: values) {
				if("order".equals(value.getTableName())) {
					BeanObject obj = new BeanObject();
					try {
						BeanUtils.copyProperties(obj, value);
					} catch (IllegalAccessException | InvocationTargetException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					arrayList.add(obj);
				}else {
					try {
						beanObject=new BeanObject();
						BeanUtils.copyProperties(beanObject, value);
					} catch (IllegalAccessException | InvocationTargetException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
			
			for (BeanObject value : arrayList) {
				value.setUserName(beanObject.getUserName());
				value.setAge(beanObject.getAge());
				value.setLover(beanObject.getLover());
				value.setTableName("left join");
				context.write(value, NullWritable.get());
			}
			
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JoinAlogrithm.class);
		
		job.setMapperClass(JoinAlogrithmMapper.class);
		job.setReducerClass(JoinAlogrithmReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BeanObject.class);
		
		job.setOutputKeyClass(BeanObject.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\joindata\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\joindata\\output"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
}
