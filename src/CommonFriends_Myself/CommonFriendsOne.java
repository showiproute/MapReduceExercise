package CommonFriends_Myself;

import java.io.IOException;
import java.util.ArrayList;

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

	
	public static class CommonFriendsOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k=new Text();
		Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String[] lines = value.toString().split(":");	
			String user= lines[0];
			v.set(user);
			String[] friends = lines[1].split(",");
			for (String friend : friends) {
				k.set(friend);
				context.write(k, v);
			}
			
		}
	}
	
	public static class CommonFriendsOneReducer extends Reducer<Text, Text, Text, Text>{
		
		Text k=new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		
			ArrayList<String> friendList = new ArrayList<>();
			for (Text friend : values) {
				friendList.add(friend.toString());
			}
			
			for(int i=0;i<friendList.size()-1;i++) {
				for(int j=i+1;j<friendList.size();j++) {
					k.set(friendList.get(i)+"-"+friendList.get(j));
					context.write(k, key);
					
				}
			}
			
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CommonFriendsOne.class);
	
		job.setMapperClass(CommonFriendsOneMapper.class);
		job.setReducerClass(CommonFriendsOneReducer.class);
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\frienddata\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\frienddata\\outputmyself"));
		
		boolean res= job.waitForCompletion(true);
		System.exit(res?0:-1);
	}
}
