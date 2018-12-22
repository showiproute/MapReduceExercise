package pagetopn;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageTopnReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	TreeMap<PageCount, Object> treemap=new TreeMap<>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int count=0;
		for(IntWritable value:values) {
			count+=value.get();
		}
		PageCount pageCount = new PageCount();
		pageCount.set(key.toString(), count);
		treemap.put(pageCount, null);
		
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = context.getConfiguration();
		int topn = conf.getInt("top.n", 5);
	
		Set<Entry<PageCount,Object>> entrySet = treemap.entrySet();
		int i=0;
		for(Entry<PageCount,Object> entry:entrySet) {
			context.write(new Text(entry.getKey().getPage()), new IntWritable(entry.getKey().getCount()));
			i++;
			if(i==topn) return ;
		}
		
	}
	
	
}
