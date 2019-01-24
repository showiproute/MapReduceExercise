package JoinMapReduce;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.tools.rumen.datatypes.FileName;

public class ReduceSlideJoin {

	public static class ReduceSlideJoinMapper extends Mapper<LongWritable, Text, Text, JoinBean>{
		
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 * maptask在数据处理时，会先调用一次setup（）方法 !!! 注意只调用一次
		 * 执行完后，再对每一行执行map（）方法
		 */
		
		private String filename;
		Text k=new Text();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			this.filename = inputSplit.getPath().getName();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, JoinBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields= value.toString().split(",");
			JoinBean bean = new JoinBean();
			
			if(filename.startsWith("order")) {
				bean.set(fields[0], fields[1], "NULL",-1, "NULL", "order");
			}else {
				bean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
			}
			
			k.set(bean.getUserId());
			context.write(k, bean);
			
		}
		
	}
	
	static class ReduceSlideJoinReducer extends Reducer<Text, JoinBean, JoinBean, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<JoinBean> beans,
				Reducer<Text, JoinBean, JoinBean, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ArrayList<JoinBean> orderList = new ArrayList<>();
			JoinBean userBean=null;
			try {
				
				//区分两类数据
				for (JoinBean bean : beans) {
					if("order".equals(bean.getTableName())) {
						JoinBean newBean = new JoinBean();
						BeanUtils.copyProperties(newBean, bean);
						
						orderList.add(newBean);
					}else {
						userBean=new JoinBean();
						BeanUtils.copyProperties(userBean, bean);
					}
				}
				//拼接数据
				for(JoinBean bean:orderList) {
					bean.setUserName(userBean.getUserName());
					bean.setUserAge(userBean.getUserAge());
					bean.setUserFriend(userBean.getUserFriend());
					
					context.write(bean, NullWritable.get());
				}
			}catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ReduceSlideJoin.class);

		job.setMapperClass(ReduceSlideJoinMapper.class);
		job.setReducerClass(ReduceSlideJoinReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinBean.class);
		
		job.setOutputKeyClass(JoinBean.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(2);

		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\joindata\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\joindata\\output2"));
		
		boolean res= job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
	
}
