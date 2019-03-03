package MRCodingTwelve_JsonDataAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonDataAnalysis {

	public static class JsonDataAnalysisMapper extends Mapper<LongWritable, Text, Text, MovieObject>{

		JsonParser parse=null;
		MovieObject v=null;
		Text k=null;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, MovieObject>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parse=new JsonParser();
			v=new MovieObject();
			k=new Text();
		}
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieObject>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			JsonObject json = (JsonObject)parse.parse(value.toString());
			v.set(json.get("movie").getAsString(), json.get("rate").getAsInt(),
					json.get("timeStamp").getAsString(), json.get("uid").getAsString());
			
			k.set(json.get("uid").getAsString());
			
			context.write(k, v);
			
		}
	}
	
	public static class JsonDataAnalysisReducer extends Reducer<Text, MovieObject, Text, MovieObject>{
		
		private int topn;
		@Override
		protected void setup(Reducer<Text, MovieObject, Text, MovieObject>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = context.getConfiguration();
			topn=conf.getInt("top.n.value", 2);
		}
		
		@Override
		protected void reduce(Text key, Iterable<MovieObject> values,
				Reducer<Text, MovieObject, Text, MovieObject>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int i=0;
			for (MovieObject value: values) {
				context.write(key, value);
				i++;
				if(i==topn) return;
				
			}
		}
		
		
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		conf.setInt("top.n.value", 3);
		
		job.setJarByClass(JsonDataAnalysis.class);
		job.setMapperClass(JsonDataAnalysisMapper.class);
		job.setReducerClass(JsonDataAnalysisReducer.class);
		
//		job.setGroupingComparatorClass(ObjectGroupingComparator.class);
//		job.setPartitionerClass(ObjectPartitioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MovieObject.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MovieObject.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.setInputPaths(job, new Path("F:\\Linuxsource\\mrdata\\jsondata\\input"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Linuxsource\\mrdata\\jsondata\\output5"));
		
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:-1);
		
	}
	
}
