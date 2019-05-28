package JoinMapReduce_myself;

import java.io.IOException;
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


public class JoinDataTask {
	
	public static class JoinDataTaskMapper extends Mapper<LongWritable, Text, Text, DataBean>{
		private String fileName;
		DataBean bean=new DataBean();
		Text k=new Text();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, DataBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit inputSplit =(FileSplit) context.getInputSplit();
			this.fileName = inputSplit.getPath().getName();
			
		}
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(fileName.startsWith("order")) {
				String[] orderFields = value.toString().split(",");
				bean.set(orderFields[0], orderFields[1], "NULL", -1, "NULL", "order");
				k.set(orderFields[1]);
				context.write(k, bean);
			}else {
				String[] userFields = value.toString().split(",");
				bean.set("NULL", userFields[0], userFields[1], Integer.parseInt(userFields[2]), userFields[3], "user");
				k.set(bean.getUserId());
				context.write(k, bean);
			}
		}
		
	}
	
	public static class JoinDataTaskReducer extends Reducer<Text, DataBean, DataBean, NullWritable>{

		
		@Override
		protected void reduce(Text key, Iterable<DataBean> dataBeans,
				Reducer<Text, DataBean, DataBean, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ArrayList<DataBean> orderList = new ArrayList<>();
			DataBean userBean=null;
			try {
				for (DataBean dataBean : dataBeans) {
					String tableName = dataBean.getTableName();
					if("order".equals(tableName)) {
						DataBean dstBean=new DataBean();
						BeanUtils.copyProperties(dstBean, dataBean);
						orderList.add(dstBean);
					}else {
						userBean=new DataBean();
						BeanUtils.copyProperties(userBean, dataBean);
					}
					
				}
				for (DataBean dataBean : orderList) {
					dataBean.setUserName(userBean.getUserName());
					dataBean.setUserAge(userBean.getUserAge());
					dataBean.setUserFriend(userBean.getUserFriend());
					
					context.write(dataBean, NullWritable.get());
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
		
		job.setJarByClass(JoinDataTask.class);

		job.setMapperClass(JoinDataTaskMapper.class);
		job.setReducerClass(JoinDataTaskReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataBean.class);
		
		job.setOutputKeyClass(DataBean.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\joindata\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\joindata\\output7"));
		
		boolean res= job.waitForCompletion(true);
		
		System.exit(res?0:-1);
	}
}
