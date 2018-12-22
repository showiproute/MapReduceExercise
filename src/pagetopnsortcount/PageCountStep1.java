package pagetopnsortcount;

import java.io.IOException;

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

public class PageCountStep1 {

	public static class PageCountStep1Mapper extends Mapper<LongWritable,Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String line = value.toString();
			String[] split = line.split(" ");
			context.write(new Text(split[1]), new IntWritable(1));
		}
	
    public static class PageCountStep1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
    	@Override
    	protected void reduce(Text key, Iterable<IntWritable> values,
    			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
    		// TODO Auto-generated method stub
    			int count=0;
    			for(IntWritable v:values) {
    				count+=v.get();
    			}
    			context.write(key, new IntWritable(count));
    		}
		}
    
    public static void main(String[] args) throws Exception {
    	
    	Configuration conf = new Configuration();
    	
    	Job job = Job.getInstance(conf);
    	
    	
    	job.setJarByClass(PageCountStep1.class);
    	
    	job.setMapperClass(PageCountStep1Mapper.class);
    	job.setReducerClass(PageCountStep1Reducer.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(IntWritable.class);
    	
    	FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/pagetopsortcount/input"));
    	FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/pagetopsortcount/output"));
    	
    	job.setNumReduceTasks(1);
    	
    	boolean res = job.waitForCompletion(true);
    	System.exit(res?0:-1);
    	
    }
    
	}
}
