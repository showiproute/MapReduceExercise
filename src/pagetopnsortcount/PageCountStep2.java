package pagetopnsortcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class PageCountStep2 {
	public static class PageCountStep2Mapper extends Mapper<LongWritable, Text, PageCount, NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, PageCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[]  split = line.split("\t");
			PageCount pageCount = new PageCount();
			pageCount.set(split[0].toString(), Integer.parseInt(split[1]));
			
			context.write(pageCount, NullWritable.get());
			
		}
		
	}
	
	public static class PageCountStep2Reducer extends Reducer<PageCount, NullWritable,
					PageCount, NullWritable>{
		@Override
		protected void reduce(PageCount key, Iterable<NullWritable> values,
				Reducer<PageCount, NullWritable, PageCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}
		
	}
	
	 public static void main(String[] args) throws Exception {
	    	
	    	Configuration conf = new Configuration();
	    	
	    	Job job = Job.getInstance(conf);
	    	
	    	
	    	job.setJarByClass(PageCountStep2.class);
	    	
	    	job.setMapperClass(PageCountStep2Mapper.class);
	    	job.setReducerClass(PageCountStep2Reducer.class);
	    	
	    	job.setOutputKeyClass(PageCount.class);
	    	job.setOutputValueClass(NullWritable.class);
	    	
	    	job.setMapOutputKeyClass(PageCount.class);
	    	job.setMapOutputValueClass(NullWritable.class);
	    	
	    	FileInputFormat.setInputPaths(job, new Path("f:/Linuxsource/mrdata/pagetopsortcount/output"));
	    	FileOutputFormat.setOutputPath(job, new Path("f:/Linuxsource/mrdata/pagetopsortcount/sortout"));
	    	
	    	job.setNumReduceTasks(1);
	    	
	    	boolean res = job.waitForCompletion(true);
	    	System.exit(res?0:-1);
	    	
	    }

}
