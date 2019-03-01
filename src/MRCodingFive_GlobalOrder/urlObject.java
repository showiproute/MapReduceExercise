package MRCodingFive_GlobalOrder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class urlObject implements WritableComparable<urlObject>{

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}
	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}
	/**
	 * @return the counts
	 */
	public int getCounts() {
		return counts;
	}
	/**
	 * @param counts the counts to set
	 */
	public void setCounts(int counts) {
		this.counts = counts;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "urlObject [url=" + url + ", counts=" + counts + "]";
	}
	
	public void set(String url, int counts) {
		this.url = url;
		this.counts = counts;
	}
	
	private String url;
	private int counts;
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.url=in.readUTF();
		this.counts=in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.url);
		out.writeInt(this.counts);
		
	}
	@Override
	public int compareTo(urlObject o) {
		// TODO Auto-generated method stub
		return this.counts-o.getCounts()==0?this.getUrl().compareTo(o.getUrl()):
			o.getCounts()-this.getCounts();
	}
	
	
}
