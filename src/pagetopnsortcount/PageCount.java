package pagetopnsortcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PageCount implements WritableComparable<PageCount>{
	private String page;
	private int count;
	
	public void set(String page,int count) {
		this.page=page;
		this.count=count;
	}
	
	/**
	 * @return the page
	 */
	public String getPage() {
		return page;
	}
	/**
	 * @param page the page to set
	 */
	public void setPage(String page) {
		this.page = page;
	}
	/**
	 * @return the count
	 */
	public int getCount() {
		return count;
	}
	/**
	 * @param count the count to set
	 */
	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public int compareTo(PageCount o) {
		// TODO Auto-generated method stub
		return (o.getCount()-this.count)==0?this.page.compareTo(o.getPage()):
			o.getCount()-this.count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.page=in.readUTF();
		this.count=in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.page);
		out.writeInt(this.count);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.page+this.count;
	}
	
	
}
