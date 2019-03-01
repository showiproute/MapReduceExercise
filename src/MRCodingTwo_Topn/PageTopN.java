package MRCodingTwo_Topn;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageTopN {

	public static class PageTopNMapper extends Mapper<LongWritable,Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String[] fields = value.toString().split(" ");
			
			context.write(new Text(fields[1]), new IntWritable(1));
		}
	}
	
	public static class PageTopNReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
			
		TreeMap<PageObject, Object> treeMap=new TreeMap<>();
		
		@Override
		protected void reduce(Text url, Iterable<IntWritable> iter,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int total=0;
			Iterator<IntWritable> iterator = iter.iterator();
			while(iterator.hasNext()) {
				total += iterator.next().get();
			}
			PageObject pageObject = new PageObject();
			pageObject.set(url.toString(), total);
			
			treeMap.put(pageObject, null);
			
		}
		
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			Configuration conf = new Configuration();
			int topn = conf.getInt("top.n.value", 3);
			int i=0;
			Set<Entry<PageObject,Object>> entrySet = treeMap.entrySet();
			for (Entry<PageObject, Object> entry : entrySet) {
				context.write(new Text(entry.getKey().getUrl()), new IntWritable(entry.getKey().getCounts()));
				i++;
				if(i==topn) return;
			}
			
			
		}
		
		public static void main(String[] args) throws Exception{
			Configuration conf = new Configuration();
			
			conf.setInt("top.n.value", 3);
			
			Job job = Job.getInstance(conf);
			job.setJarByClass(PageTopN.class);
			
			job.setMapperClass(PageTopNMapper.class);
			job.setReducerClass(PageTopNReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setNumReduceTasks(1);
			
			FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/topn/input"));
			FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/topn/output2"));
			
			boolean res = job.waitForCompletion(true);
			
			System.exit(res?0:-1);
			
		}
		
	}
	
}
