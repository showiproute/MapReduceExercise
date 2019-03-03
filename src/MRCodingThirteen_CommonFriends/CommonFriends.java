package MRCodingThirteen_CommonFriends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriends {

	public static class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		/*
		 *输入数据 A：B,C,D,E,F,O
		 *输出数据 B->A C->A D->A ....
		 */
		Text k=new Text();
		Text v=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] fields = value.toString().split(":");
			v.set(fields[0]);
			String[] words = fields[1].split(",");
			for (String  word:words ) {
				k.set(word);
				context.write(k, v);
			}
			
		}
	}
	
	public static class CommonFriendsReducer extends Reducer<Text, Text, Text, Text>{
		
		/*
		 * 输入数据： B->A  B-> E B->F
		 * 		   
		 * 输出数据 A&E commonfriend B A&F commonfriend B .......
		*/
		
		Text k=new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			ArrayList<String> arrayList = new ArrayList<>();
			
			for (Text value : values) {
				arrayList.add(value.toString());
			}
			Collections.sort(arrayList);
			
			for(int i=0;i<arrayList.size()-1;i++) 
				for(int j=1;j<arrayList.size()-1;j++) {
					StringBuilder sb = new StringBuilder();
					sb.append(arrayList.get(i)).append("&").append(arrayList.get(j)).append("CommonFreind:");
					k.set(sb.toString());
					context.write(k,key);
				}
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CommonFriends.class);
		job.setMapperClass(CommonFriendsMapper.class);
		job.setReducerClass(CommonFriendsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\frienddata\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\frienddata\\output"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:-1);
		
	}
}
