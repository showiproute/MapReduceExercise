package pagetopn_myself;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * cleanup function()
 */
public class PagetopnReducer_myself extends Reducer<Text, IntWritable, Text, IntWritable>{

	static TreeMap<PageCount_myself,Object> treemap=new TreeMap<>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		PageCount_myself pageCount_myself = new PageCount_myself();
		int count=0;
		for(IntWritable value:values) {
			count+=value.get();
			
		}
		pageCount_myself.setCount(count);
		pageCount_myself.setPage(key.toString());

		treemap.put(pageCount_myself, null);
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = context.getConfiguration();
		int topnum = conf.getInt("top.n", 5);
		Set<Entry<PageCount_myself,Object>> entrySet = treemap.entrySet();
		int i=0;
		for(Entry<PageCount_myself,Object> entry:entrySet) {
			context.write(new Text(entry.getKey().getPage()), new IntWritable(entry.getKey().getCount()));
			i++;
			if(i==5) return;
		}
		
	}
}
