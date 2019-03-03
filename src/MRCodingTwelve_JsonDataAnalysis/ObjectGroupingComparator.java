package MRCodingTwelve_JsonDataAnalysis;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ObjectGroupingComparator extends WritableComparator{
	public ObjectGroupingComparator() {
		// TODO Auto-generated constructor stub
		super(MovieObject.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		
		MovieObject object1=(MovieObject)a;
		MovieObject object2=(MovieObject)b;
		
		return object1.getUid().compareTo(object2.getUid());
	}
	
}
