package CommonFriends;

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

public class CommonFriendsOne {

	public static class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		//A: B,C,D,F,E,O
		//输出: B->A C->A D->A...
		
		Text k=new Text();
		Text v=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] userAndFriends = value.toString().split(":");
			
			String user = userAndFriends[0];
			v.set(user);
			String[] friends=userAndFriends[1].split(",");
			for (String f : friends) {
				k.set(f);
				context.write(k, v);
			}
			
			
		}
	}
	
	public static class CommonFriendsReducer extends Reducer<Text, Text, Text, Text>{
		
		//一组数据: B--> A E F J ....
		//一组数据: C--> B F E J ....
		@Override
		protected void reduce(Text friend, Iterable<Text> users, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ArrayList<String> userList = new ArrayList<>();
			for(Text user:users) {
				
				userList.add(user.toString());
			}
			
			Collections.sort(userList);
			
			for(int i=0;i<userList.size()-1;i++) {
				for(int j=i+1;j<userList.size();j++) {
					context.write(new Text(userList.get(i)+"-"+userList.get(j)), friend);
				}	
			}
			
			
		}
		
	}
	
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(CommonFriendsOne.class);
		
		job.setMapperClass(CommonFriendsMapper.class);
		job.setReducerClass(CommonFriendsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\frienddata\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\frienddata\\output"));
		
		boolean res= job.waitForCompletion(true);
		System.exit(res?0:-1);
	
	}
	
}
